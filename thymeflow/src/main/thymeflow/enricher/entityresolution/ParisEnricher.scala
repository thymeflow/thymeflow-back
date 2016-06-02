package thymeflow.enricher.entityresolution

import java.nio.file.Paths

import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{IRI, Resource}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import thymeflow.actors._
import thymeflow.enricher.AbstractEnricher
import thymeflow.enricher.entityresolution.EntityResolution.{LevensteinSimilarity, StringSimilarity}
import thymeflow.enricher.entityresolution.ParisEnricher.{EqualityStore, ParisLiteral}
import thymeflow.graph.ConnectedComponents
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.ModelDiff
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.text.search.elasticsearch.FullTextSearchServer
import thymeflow.utilities.TimeExecution

import scala.collection.immutable.NumericRange
import scala.concurrent.Await

/**
  * @author David Montoya
  */
/**
  *
  * @param repositoryConnection           a connection to the knowledge base
  * @param baseStringSimilarity           the base string similarity
  * @param searchSize                     the number of candidate results for each term
  * @param searchMatchPercent             the term percent match required for candidate equalities
  * @param useIDF                         use Term IDF adjustment
  * @param matchDistanceThreshold         maximum distance for a term to be considered as match
  * @param evaluationSamplesFiles         sample files (with ground truth) to evaluate from
  * @param parallelism                    the concurrency level when looking up for candidate equalities
  * @param persistenceThreshold           probability threshold above which equalities are saved
  * @param propertiesInverseFunctionality a map from properties to their inverse functionality value
  * @param propertiesFunctionality        a map from properties to their functionality value
  */
class ParisEnricher(repositoryConnection: RepositoryConnection,
                    protected val baseStringSimilarity: StringSimilarity = LevensteinSimilarity,
                    searchSize: Int = 10000,
                    searchMatchPercent: Int = 70,
                    useIDF: Boolean = true,
                    protected val matchDistanceThreshold: Double = 0.3d,
                    evaluationSamplesFiles: IndexedSeq[String] = IndexedSeq.empty,
                    parallelism: Int = 2,
                    persistenceThreshold: BigDecimal = BigDecimal(0.9),
                    propertiesInverseFunctionality: Map[Resource, Double] = Map(
                      SchemaOrg.NAME -> 0.9700722394220846,
                      SchemaOrg.EMAIL -> 0.99),
                    propertiesFunctionality: Map[Resource, Double] = Map(
                      SchemaOrg.EMAIL -> 0.8731440162271805,
                      SchemaOrg.NAME -> 0.8043465064044194)
                   ) extends AbstractEnricher(repositoryConnection) with EntityResolutionEvaluation with StrictLogging {


  protected val outputFilePrefix = "data/paris-enricher"
  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "ParisEnricher")

  private val agentNamesQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?agent ?name WHERE {
      ?agent a <${Personal.AGENT}> ;
               <${SchemaOrg.NAME}> ?name .
    }"""
  )
  private val agentEmailAddressesQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?agent ?emailAddress WHERE {
       ?agent a <${Personal.AGENT}> ;
              <${SchemaOrg.EMAIL}>/<${SchemaOrg.NAME}> ?emailAddress .
    }"""
  )
  var literalId = 0

  /**
    * @return enrichs the repository the Enricher is linked to based on the diff
    */
  override def enrich(diff: ModelDiff): Unit = {
    val agentNames = getAgentNames
    val agentEmails = getAgentEmails
    val literalToAgent = (agentNames.toIndexedSeq.flatMap {
      case (agent, literals) =>
        literals.map(literal => literal ->(SchemaOrg.NAME, agent))
    } ++ agentEmails.toIndexedSeq.flatMap {
      case (agent, literals) =>
        literals.map(literal => literal ->(SchemaOrg.EMAIL, agent))
    }).toMap
    val agents = (agentNames.keySet ++ agentEmails.keySet).toIndexedSeq
    def statementsFrom(agent: Resource, property: Option[Resource]) = {
      property match {
        case Some(SchemaOrg.EMAIL) => agentEmails(agent).map(x => (agent, SchemaOrg.EMAIL, Right(x)))
        case Some(SchemaOrg.NAME) => agentNames(agent).map(x => (agent, SchemaOrg.NAME, Right(x)))
        case None => agentEmails(agent).map(x => (agent, SchemaOrg.EMAIL, Right(x))) ++ agentNames(agent).map(x => (agent, SchemaOrg.NAME, Right(x)))
        case _ => IndexedSeq.empty
      }
    }
    def statementsTo(agent: Either[Resource, ParisLiteral], property: Resource) = {
      agent match {
        case Left(_) => IndexedSeq.empty
        case Right(literal) =>
          val agent = literalToAgent(literal)
          if (property == agent._1) {
            IndexedSeq((agent._2, agent._1, Right(literal)))
          } else {
            IndexedSeq.empty
          }
      }
    }
    val literalIdToLiteral = literalToAgent.keySet.map {
      case literal => literal.id -> literal
    }.toMap

    val termIDFs: String => Double = if (useIDF) {
      computeTermIDFs(literalToAgent.keys.view.map(x => x.terms)).compose(normalizeTerm)
    } else {
      _ => 1d
    }

    val result = FullTextSearchServer[ParisLiteral](x => literalIdToLiteral.apply(x.toInt))(_.id.toString, searchSize = searchSize).flatMap {
      case textSearchServer =>
        // add all terms to the index
        textSearchServer.add(literalToAgent.keys.map {
          case literal => (literal, literal.value)
        }).flatMap {
          _ => textSearchServer.refreshIndex()
        }.map {
          _ => textSearchServer
        }
    }.flatMap {
      textSearchServer =>
        val literalEqualityStoreBuilder = EqualityStore.newBuilder[ParisLiteral]()
        Source.fromIterator(() => literalToAgent.keysIterator).mapAsync(parallelism) {
          case literal =>
            // search for agents containing term or similar
            textSearchServer.matchQuery(literal.value, matchPercent = searchMatchPercent).map {
              hits =>
                (literal, hits)
            }
        }.runForeach {
          case (literal1, hits) =>
            // build a list of (literal1, literal2) equalities
            hits.foreach {
              case (literal2, _, _) if literal2 != literal1 =>
                val probability = getNameTermsEqualityProbability(literal1.terms, literal2.terms, termIDFs, getMatchingTermIndicesWithSimilarities)
                literalEqualityStoreBuilder += ((literal1, literal2, probability))
              case _ =>
            }
        }.map {
          done =>
            val equalities = run[Resource, IRI, ParisLiteral](propertiesInverseFunctionality.apply,
              propertiesFunctionality.apply,
              agents,
              statementsFrom,
              statementsTo,
              literalEqualityStoreBuilder.result(),
              10)

            lazy val buckets = samplingBuckets()
            lazy val equivalentClasses = generateEquivalentClasses(buckets, equalities.definedEqualities)
            evaluationSamplesFiles.foreach {
              path =>
                val uriStringToResource = agents.view.map {
                  agent =>
                    (agent.stringValue(), agent)
                }.toMap.withDefault(repositoryConnection.getValueFactory.createIRI(_))
                val samples = parseSamplesFromFile(path, uriStringToResource)
                val evaluatedSamples = evaluateSamples(samples, equivalentClasses)
                saveEvaluationToFile(s"${Paths.get(path).getFileName.toString}_SMP${searchMatchPercent}_MDT${matchDistanceThreshold}_SS${searchSize}_BSD{$baseStringSimilarity}_IDF$useIDF",
                  evaluatedSamples)
            }
            repositoryConnection.begin()
            equalities.definedEqualities.filterNot(tuple => isDifferentFrom(tuple._1, tuple._2)).foreach {
              case (agent1, agent2, probability) if probability >= persistenceThreshold =>
                addStatement(diff, valueFactory.createStatement(agent1, Personal.SAME_AS, agent2, inferencerContext))
                addStatement(diff, valueFactory.createStatement(agent2, Personal.SAME_AS, agent1, inferencerContext))
              case _ =>
            }
            repositoryConnection.commit()
        }
    }
    result.onFailure {
      case throwable => logger.error(thymeflow.utilities.ExceptionUtils.getUnrolledStackTrace(throwable))
    }
    Await.ready(result, scala.concurrent.duration.Duration.Inf)
  }

  def run[INSTANCE, PROPERTY, LITERAL](propertyInverseFunctionality: PROPERTY => Double,
                                       propertyFunctionality: PROPERTY => Double,
                                       instances: IndexedSeq[INSTANCE],
                                       statementsFrom: (INSTANCE, Option[PROPERTY]) => Traversable[(INSTANCE, PROPERTY, Either[INSTANCE, LITERAL])],
                                       statementsTo: (Either[INSTANCE, LITERAL], PROPERTY) => Traversable[(INSTANCE, PROPERTY, Either[INSTANCE, LITERAL])],
                                       literalEqualities: EqualityStore[LITERAL],
                                       iterations: Int) = {
    (0 until iterations).foldLeft(EqualityStore.newBuilder[INSTANCE]().result()) {
      case (instanceEqualities, i) =>
        logger.info(s"PARIS: iteration $i")
        step(propertyInverseFunctionality,
          propertyFunctionality,
          instances,
          statementsFrom,
          statementsTo,
          literalEqualities,
          instanceEqualities)
    }
  }

  def step[INSTANCE, PROPERTY, LITERAL](propertyInverseFunctionality: PROPERTY => Double,
                                        propertyFunctionality: PROPERTY => Double,
                                        instances: IndexedSeq[INSTANCE],
                                        statementsFrom: (INSTANCE, Option[PROPERTY]) => Traversable[(INSTANCE, PROPERTY, Either[INSTANCE, LITERAL])],
                                        statementsTo: (Either[INSTANCE, LITERAL], PROPERTY) => Traversable[(INSTANCE, PROPERTY, Either[INSTANCE, LITERAL])],
                                        literalEqualities: EqualityStore[LITERAL],
                                        instanceEqualities: EqualityStore[INSTANCE]): EqualityStore[INSTANCE] = {
    val builder = EqualityStore.newBuilder[INSTANCE]()
    val total = instances.size
    TimeExecution.timeProgressStep("paris-step", total, logger, {
      progress =>
        for (x <- instances) {
          val statementsFromX = statementsFrom(x, None)
          val candidateXPrimes = statementsFromX.flatMap {
            case (_, predicate, obj) if (propertyInverseFunctionality(predicate) > 0.0) || (propertyFunctionality(predicate) > 0.0) =>
              val equalityScores = obj match {
                case Left(candidateInstance) =>
                  instanceEqualities.strictlyPositiveEqualityScores(candidateInstance).map {
                    case (instance2, score) => (Left(instance2), score)
                  }
                case Right(literal) =>
                  literalEqualities.strictlyPositiveEqualityScores(literal).map {
                    case (literal2, score) => (Right(literal2), score)
                  }
                case _ => Vector.empty
              }

              equalityScores.view.flatMap {
                case (yPrime, score) =>
                  statementsTo(yPrime, predicate).map(_._1)
              }
            case _ => IndexedSeq.empty
          }.toSet
          for (xPrime <- candidateXPrimes) {
            val positiveEvidence = statementsFromX.view.collect {
              case (_, predicate, yResource) if propertyInverseFunctionality(predicate) > 0.0 =>
                val inverseFunctionality = propertyInverseFunctionality(predicate)
                statementsFrom(xPrime, Some(predicate)).view.map {
                  case (_, _, obj) =>
                    val equalityScore = (yResource, obj) match {
                      case (Left(y), Left(yPrime)) => instanceEqualities.equality(y, yPrime)
                      case (Right(y), Right(yPrime)) => literalEqualities.equality(y, yPrime)
                      case _ => 0d
                    }
                    1d - inverseFunctionality * equalityScore
                }.product
            }.product
            val negativeEvidence = statementsFromX.view.collect {
              case (_, predicate, yResource) if propertyFunctionality(predicate) > 0.0 =>
                val functionality = propertyFunctionality(predicate)
                1d - functionality * statementsFrom(xPrime, Some(predicate)).view.map {
                  case (_, _, obj) =>
                    val equalityScore = (yResource, obj) match {
                      case (Left(y), Left(yPrime)) => instanceEqualities.equality(y, yPrime)
                      case (Right(y), Right(yPrime)) => literalEqualities.equality(y, yPrime)
                      case _ => 0d
                    }
                    1d - equalityScore
                }.product
            }.product
            builder += ((x, xPrime, (1d - positiveEvidence) * negativeEvidence))
          }
          progress()
        }
        builder.result()
    })
  }

  private def generateEquivalentClasses[INSTANCE](buckets: NumericRange[BigDecimal],
                                                  equalities: Traversable[(INSTANCE, INSTANCE, Double)]): Seq[(Option[BigDecimal], Map[INSTANCE, Set[INSTANCE]], IndexedSeq[Set[INSTANCE]])] = {
    buckets.reverse.view.map {
      threshold =>
        val (map, classes) = equalityRelationEquivalentClasses(equalities, threshold)
        (Some(threshold), map, classes)
    }
  }

  private def equalityRelationEquivalentClasses[INSTANCE](equalities: Traversable[(INSTANCE, INSTANCE, Double)],
                                                          threshold: BigDecimal) = {
    val e = equalities.view.filter(_._3 >= threshold).map(x => (x._1, x._2))
    val neighbors = (e ++ e.map(x => (x._2, x._1))).groupBy(_._1).map {
      case (instance1, instances) => (instance1, instances.map(_._2).toSet)
    }
    val connectedComponents = ConnectedComponents.compute[INSTANCE](neighbors.keys, neighbors.getOrElse(_, Traversable.empty))

    (connectedComponents.flatMap(connectedComponent => connectedComponent.view.map {
      _ -> connectedComponent
    })
      .toMap
      .withDefault(Set(_)), connectedComponents)
  }

  private def getAgentNames = {
    agentNamesQuery.evaluate().toIterator.map(bindingSet =>
      (Option(bindingSet.getValue("agent").asInstanceOf[Resource]), Option(bindingSet.getValue("name")).map(_.stringValue()))
    ).collect {
      case (Some(agent), Some(name)) => (agent, name)
    }.toTraversable.groupBy(_._1).map {
      case (agent, g) => (agent, g.map { x => literalId += 1
        new ParisLiteral(literalId, x._2, extractTerms(x._2).map(x => (normalizeTerm(x), 1d)))
      }.toIndexedSeq)
    }.withDefaultValue(IndexedSeq.empty)
  }

  private def getAgentEmails = {
    agentEmailAddressesQuery.evaluate().toIterator.map(bindingSet =>
      (Option(bindingSet.getValue("agent").asInstanceOf[Resource]), Option(bindingSet.getValue("emailAddress")).map(_.stringValue()))
    ).collect {
      case (Some(agent), Some(emailAddress)) => (agent, emailAddress)
    }.toTraversable.groupBy(_._1).map {
      case (agent, g) => (agent, g.map { x => literalId += 1
        new ParisLiteral(literalId, x._2, extractTerms(x._2).map(x => (normalizeTerm(x), 1d)))
      }.toIndexedSeq)
    }.withDefaultValue(IndexedSeq.empty)
  }


}

object ParisEnricher {

  trait EqualityStore[T] {
    def equality(t1: T, t2: T): Double

    def strictlyPositiveEqualityScores(t: T): Traversable[(T, Double)]

    def definedEqualities: Traversable[(T, T, Double)]
  }

  class ParisLiteral(val id: Int, val value: String, val terms: IndexedSeq[(String, Double)])

  class EqualityStoreImpl[T](equalityMap: scala.collection.mutable.HashMap[T, scala.collection.mutable.HashMap[T, Double]], selfEquality: Boolean = true)
    extends EqualityStore[T] {

    override def equality(t1: T, t2: T): Double = {
      if (selfEquality && (t1 == t2)) {
        1d
      } else {
        // the map returns its default value 0d if (t1, t2) is not present
        equalityMap.get(t1).flatMap(_.get(t2)).getOrElse(0d)
      }
    }

    override def strictlyPositiveEqualityScores(t1: T): Traversable[(T, Double)] = {
      equalityMap.get(t1).map {
        x => x.view.collect {
          case (t2, score) if score > 0d => (t2, score)
        }.toIndexedSeq
      }.getOrElse(IndexedSeq.empty)
    }

    override def toString: String = {
      definedEqualities.toString()
    }

    override def definedEqualities: Traversable[(T, T, Double)] = {
      equalityMap.view.flatMap {
        case (t1, m) => m.map {
          case (t2, score) => (t1, t2, score)
        }
      }.toIndexedSeq
    }
  }

  object EqualityStore {
    def newBuilder[T]() = {
      val equalityMap = new scala.collection.mutable.HashMap[T, scala.collection.mutable.HashMap[T, Double]]
      new scala.collection.mutable.Builder[(T, T, Double), EqualityStore[T]] {
        override def +=(elem: (T, T, Double)): this.type = {
          def getMap(t: T) = {
            equalityMap.getOrElse(t, {
              val equalityMapForT = new scala.collection.mutable.HashMap[T, Double]
              equalityMap += ((t, equalityMapForT))
              equalityMapForT
            })
          }
          elem match {
            case (t1, t2, score) =>
              val t1Map = getMap(t1)
              val t2Map = getMap(t2)
              t1Map += ((t1, score))
              t2Map += ((t1, score))
          }
          this
        }

        override def result(): EqualityStore[T] = {
          new EqualityStoreImpl[T](equalityMap)
        }

        override def clear(): Unit = equalityMap.clear()
      }
    }
  }

}