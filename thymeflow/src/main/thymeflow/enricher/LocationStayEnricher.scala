package thymeflow.enricher

import java.time.{Duration, Instant}

import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{IRI, Literal, Resource}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import thymeflow.actors._
import thymeflow.location.Clustering
import thymeflow.location.cluster.MaxLikelihoodCluster
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.spatial.geographic.{Geography, Point}
import thymeflow.sync.converter.utils.GeoCoordinatesConverter
import thymeflow.utilities.{ExceptionUtils, TimeExecution}

import scala.concurrent.Future

/**
  * @author David Montoya
  */
class LocationStayEnricher(repositoryConnection: RepositoryConnection, val delay: scala.concurrent.duration.Duration)
  extends DelayedEnricher with StrictLogging {


  private val valueFactory = repositoryConnection.getValueFactory
  private val geoCoordinatesConverter = new GeoCoordinatesConverter(valueFactory)

  private val inferencerContext = valueFactory.createIRI("http://thymeflow.com/personal#LocationStayStopEnricher")
  private val tempInferencerContext = valueFactory.createIRI("http://thymeflow.com/personal#LocationStayStopEnricherTemp")

  override def runEnrichments(): Unit = {
    implicit val format = org.json4s.DefaultFormats
    val clustering = new Clustering {}
    val minimumStayDuration = Duration.ofMinutes(15)
    val observationEstimatorDuration = Duration.ofMinutes(60)
    val movementEstimatorDuration = Duration.ofMinutes(120)
    val lambda = 0.95
    logger.info(s"[location-stay-enricher] - Extracting StayEvents {minimumStayDuration=$minimumStayDuration,observationEstimatorDuration=$observationEstimatorDuration,movementEstimatorDuration=$movementEstimatorDuration}.")

    countLocations.flatMap {
      locationCount =>
        val stage1 = new BufferedProcessorStage(
          (out: MaxLikelihoodCluster[Location, Instant] => Unit) => {
            val (onObservation, onFinish, _) = clustering.extractStaysFromObservations(minimumStayDuration, observationEstimatorDuration, lambda)(out)
            (onObservation, onFinish)
          }
        )
        val stage2 = new BufferedProcessorStage(
          (out: IndexedSeq[(Location, Option[ClusterObservation])] => Unit) => {
            val (onObservation, onFinish) = clustering.splitMovement(movementEstimatorDuration, lambda)(out)
            (onObservation, onFinish)
          }
        )
        val stage3 = new BufferedProcessorStage(
          (out: MaxLikelihoodCluster[Location, Instant] => Unit) => {
            val (onObservation, onFinish, _) = clustering.extractStaysFromObservations(Duration.ZERO, Duration.ofMinutes(0), lambda)(out)
            (onObservation, onFinish)
          }
        )
        getLocations.via(new TimeStage("location-stay-enricher-stage-1", locationCount)).via(stage1).map {
          case cluster =>
            (ClusterObservation(resource = valueFactory.createBNode(),
              from = cluster.observations.head.time,
              to = cluster.observations.last.time,
              accuracy = cluster.accuracy,
              point = cluster.mean), cluster.observations)
        }.runForeach {
          case (cluster, locations) =>
            // clusters are temporary
            // TODO: save them in some temporary storage
            createCluster(cluster, locations, Personal.CLUSTER_EVENT, tempInferencerContext)
        }.map {
          _ =>
            deleteGraph(inferencerContext) //We only need to delete the previously added inferences at this stage
            getLocationsWithCluster.via(new TimeStage("location-stay-enricher-stage-2", locationCount)).via(stage2).mapConcat {
              case (observationsAndClusters) =>
                clustering.estimateMovement(movementEstimatorDuration)(observationsAndClusters).getOrElse(IndexedSeq.empty).toIndexedSeq
            }.sliding(2).collect {
              case Seq(a, b) if a.resource != b.resource => a
              case Seq(a) => a
            }.via(stage3).map {
              case cluster =>
                (ClusterObservation(resource = valueFactory.createBNode(),
                  from = cluster.observations.head.time,
                  to = cluster.observations.last.time,
                  accuracy = cluster.accuracy,
                  point = cluster.mean), cluster.observations)
            }.runForeach {
              case (cluster, locations) =>
                createStay(cluster, locations)
            }.recover {
              case throwable =>
                logger.error(ExceptionUtils.getUnrolledStackTrace(throwable))
                Done
            }.foreach {
              case _ =>
                // clean-up clusters
                deleteTempInferencerGraph()
            }
        }
    }.map {
      case _ =>
        logger.info("[location-stay-enricher] - Done extracting Location StayEvents.")
    }
  }

  private def deleteTempInferencerGraph() = {
    deleteGraph(tempInferencerContext)
  }

  private def countLocations: Future[Long] = {
    Future {
      val countLocationsQuery =
        s"""
           |SELECT (count(?location) as ?count)
           |WHERE {
           |  ?location a <${Personal.TIME_GEO_LOCATION}> .
           | }
      """.stripMargin
      repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, countLocationsQuery).evaluate().map {
        case bindingSet => bindingSet.getValue("count").asInstanceOf[Literal].longValue()
      }.toVector.head
    }
  }

  private def getLocations: Source[Location, NotUsed] = {
    val locationsQuery =
      s"""
         |SELECT ?location ?time ?longitude ?latitude ?uncertainty
         |WHERE {
         |  ?location a <${Personal.TIME_GEO_LOCATION}> ;
         |            <${SchemaOrg.GEO}> ?geo ;
         |            <${SchemaOrg.DATE_CREATED}> ?time .
         |  ?geo <${SchemaOrg.LATITUDE}> ?latitude ;
         |       <${SchemaOrg.LONGITUDE}> ?longitude ;
         |       <${Personal.UNCERTAINTY}> ?uncertainty .
         | } ORDER BY ?time
      """.stripMargin
    Source.fromIterator(() => repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, locationsQuery).evaluate()).map {
      case bindingSet =>
        try {
          val longitude = bindingSet.getValue("longitude").asInstanceOf[Literal].doubleValue()
          val latitude = bindingSet.getValue("latitude").asInstanceOf[Literal].doubleValue()
          val point = Geography.point(longitude, latitude)
          Some(Location(resource = bindingSet.getValue("location").asInstanceOf[Resource],
            time = Instant.parse(bindingSet.getValue("time").stringValue()),
            point = point,
            accuracy = bindingSet.getValue("uncertainty").asInstanceOf[Literal].doubleValue()))
        } catch {
          case e: NumberFormatException => None
          case e: NullPointerException => None
        }
    }.collect {
      case Some(x) => x
    }
  }

  private def createStay(stay: ClusterObservation, locations: Traversable[Location]) = {
    createCluster(stay, locations, Personal.STAY_EVENT, inferencerContext)
  }

  private def createCluster(cluster: ClusterObservation, locations: Traversable[Location], `type`: IRI, context: IRI) = {
    repositoryConnection.begin()
    val model = new SimpleHashModel()
    val clusterGeoResource = geoCoordinatesConverter.convert(cluster.point.longitude, cluster.point.latitude, None, Some(cluster.accuracy), model)
    model.add(cluster.resource, RDF.TYPE, `type`, context)
    model.add(cluster.resource, SchemaOrg.START_DATE, valueFactory.createLiteral(cluster.from.toString, XMLSchema.DATETIME), context)
    model.add(cluster.resource, SchemaOrg.END_DATE, valueFactory.createLiteral(cluster.to.toString, XMLSchema.DATETIME), context)
    model.add(cluster.resource, SchemaOrg.GEO, clusterGeoResource, context)
    locations.foreach(location =>
      model.add(location.resource, SchemaOrg.ITEM, cluster.resource, context)
    )
    repositoryConnection.add(model)
    repositoryConnection.commit()
  }

  private def getLocationsWithCluster: Source[(Location, Option[ClusterObservation]), NotUsed] = {
    val locationsQuery =
      s"""
         |SELECT ?location ?time ?longitude ?latitude ?uncertainty ?cluster ?clusterLongitude ?clusterLatitude ?clusterFrom ?clusterTo ?clusterUncertainty
         |WHERE {
         |  ?location a <${Personal.TIME_GEO_LOCATION}> ;
         |            <${SchemaOrg.GEO}> ?geo ;
         |            <${SchemaOrg.DATE_CREATED}> ?time .
         |  ?geo <${SchemaOrg.LATITUDE}> ?latitude ;
         |       <${SchemaOrg.LONGITUDE}> ?longitude ;
         |       <${Personal.UNCERTAINTY}> ?uncertainty .
         |  OPTIONAL {
         |       ?location <${SchemaOrg.ITEM}> ?cluster .
         |       ?cluster a <${Personal.CLUSTER_EVENT}> ;
         |                <${SchemaOrg.GEO}> ?clusterGeo ;
         |                <${SchemaOrg.START_DATE}> ?clusterFrom ;
         |                <${SchemaOrg.END_DATE}> ?clusterTo .
         |       ?clusterGeo <${SchemaOrg.LONGITUDE}> ?clusterLongitude ;
         |                   <${SchemaOrg.LATITUDE}> ?clusterLatitude ;
         |                   <${Personal.UNCERTAINTY}> ?clusterUncertainty .
         |  }
         | } ORDER BY ?time
      """.stripMargin
    Source.fromIterator(() => repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, locationsQuery).evaluate()).map {
      case bindingSet =>
        try {
          val longitude = bindingSet.getValue("longitude").asInstanceOf[Literal].doubleValue()
          val latitude = bindingSet.getValue("latitude").asInstanceOf[Literal].doubleValue()
          val point = Geography.point(longitude, latitude)

          Some(Location(resource = bindingSet.getValue("location").asInstanceOf[Resource],
            time = Instant.parse(bindingSet.getValue("time").stringValue()),
            point = point,
            accuracy = bindingSet.getValue("uncertainty").asInstanceOf[Literal].doubleValue()),
            Option(bindingSet.getValue("cluster").asInstanceOf[Resource]).map {
              case cluster => ClusterObservation(resource = cluster,
                from = Instant.parse(bindingSet.getValue("clusterFrom").stringValue()),
                to = Instant.parse(bindingSet.getValue("clusterTo").stringValue()),
                point = Geography.point(bindingSet.getValue("clusterLongitude").asInstanceOf[Literal].doubleValue(), bindingSet.getValue("clusterLatitude").asInstanceOf[Literal].doubleValue()),
                accuracy = bindingSet.getValue("clusterUncertainty").asInstanceOf[Literal].doubleValue())
            })
        } catch {
          case e: NumberFormatException => None
          case e: NullPointerException => None
        }
    }.collect {
      case Some(x) => x
    }
  }

  private def deleteInferencerGraph() = {
    deleteGraph(inferencerContext)
  }

  private def deleteGraph(iri: IRI) = {
    repositoryConnection.begin()
    repositoryConnection.remove(null: Resource, null, null, iri)
    repositoryConnection.commit()
  }

  private class TimeStage[T](processName: String, target: Long, step: Long = 1) extends GraphStage[FlowShape[T, T]] {

    val in = Inlet[T]("Time.in")
    val out = Outlet[T]("Time.out")
    val shape = FlowShape.of(in, out)

    private val progress = TimeExecution.timeProgress(processName, target, logger, identity)
    private var counter = 0L

    override def createLogic(attr: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            push(out, elem)
            counter += step
            progress(counter)
          }
        })
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }
        })
      }
  }

  private class BufferedProcessorStage[A, B](processor: (B => Unit) => (A => Unit, () => Unit)) extends GraphStage[FlowShape[A, B]] {

    val in = Inlet[A]("BufferedStage.in")
    val out = Outlet[B]("BufferedStage.out")
    val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      val buffer = new scala.collection.mutable.Queue[B]()
      val (p, onFinish) = processor(buffer.enqueue(_))
      new GraphStageLogic(shape) {
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            p(elem)
            if (buffer.nonEmpty) {
              push(out, buffer.dequeue())
            } else {
              pull(in)
            }
          }

          override def onUpstreamFinish(): Unit = {
            onFinish()
            if (isAvailable(out) && !isClosed(out)) {
              if (buffer.nonEmpty) {
                push(out, buffer.dequeue())
              }
            }
            super.onUpstreamFinish()
          }
        })
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (buffer.nonEmpty) {
              push(out, buffer.dequeue())
            } else {
              pull(in)
            }
          }
        })
      }
    }
  }

  private case class Location(resource: Resource,
                              time: Instant,
                              accuracy: Double,
                              point: Point) extends thymeflow.location.treillis.Observation

  private case class ClusterObservation(resource: Resource,
                                        from: Instant,
                                        to: Instant,
                                        accuracy: Double,
                                        point: Point) extends thymeflow.location.treillis.ClusterObservation {
    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case other: ClusterObservation => other.resource == resource
        case _ => false
      }
    }

    override def hashCode(): Int = resource.hashCode()
  }
}
