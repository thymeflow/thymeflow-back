package com.thymeflow

import java.net.URLEncoder

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import akka.stream.{SourceShape, _}
import akka.util.Timeout
import com.thymeflow.Supervisor._
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import com.thymeflow.rdf.repository.Repository
import com.thymeflow.service._
import com.thymeflow.sync.Synchronizer
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.update.UpdateResults
import com.thymeflow.utilities.VectorExtensions
import com.typesafe.config.Config
import org.eclipse.rdf4j.model.vocabulary.RDF

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Try

/**
  * @author David Montoya
  */
class Supervisor(config: Config,
                 materializer: Materializer,
                 repository: Repository,
                 synchronizers: Seq[Synchronizer],
                 enrichers: Graph[FlowShape[StatementSetDiff, StatementSetDiff], _]) extends Actor {
  protected var taskCounter = 0L
  protected val taskMap = new scala.collection.mutable.HashMap[ServiceAccountSource, (Long, ServiceAccountSourceTask[_])]

  require(synchronizers.nonEmpty, "Supervisor requires at least one synchronizer")

  val connection = repository.newConnection()

  def createPipeline() = {
    val sources = synchronizers
      .map(_.source(repository.valueFactory, self)(config))
      .map(Source.fromGraph(_).mapMaterializedValue(List(_)))

    val source = VectorExtensions.reduceLeftTree(sources.toVector)(mergeSources)

    Pipeline(repository.newConnection(), source, enrichers)(materializer)
  }

  final val pipeline = createPipeline()

  private def mergeSources[Out, Mat](s1: Source[Out, List[Mat]], s2: Source[Out, List[Mat]]): Source[Out, List[Mat]] = {
    Source.fromGraph[Out, List[Mat]](GraphDSL.create(s1, s2)(_ ++ _) { implicit builder => (s1, s2) =>
      val merge = builder.add(Merge[Out](2))
      s1 ~> merge
      s2 ~> merge
      SourceShape(merge.out)
    })
  }

  def serviceIRI(service: Service) = {
    repository.valueFactory.createIRI(Personal.SERVICE.toString, "/" + URLEncoder.encode(service.name, "UTF-8"))
  }

  def serviceAccountModel(serviceAccount: ServiceAccount) = {
    val model = StatementSet.empty(repository.valueFactory)
    val serviceNode = serviceIRI(serviceAccount.service)
    val accountNode = repository.valueFactory.createIRI(serviceNode.toString, "/" + URLEncoder.encode(serviceAccount.accountId, "UTF-8"))
    model.add(accountNode, RDF.TYPE, Personal.SERVICE_ACCOUNT)
    model.add(accountNode, SchemaOrg.NAME, repository.valueFactory.createLiteral(serviceAccount.accountId))
    model.add(accountNode, Personal.ACCOUNT_OF, serviceNode)
    val sources = serviceAccount.sources.toVector.map {
      case (sourceName, source) =>
        val sourceNode = repository.valueFactory.createIRI(accountNode.toString, "/" + URLEncoder.encode(sourceName, "UTF-8"))
        model.add(sourceNode, RDF.TYPE, Personal.SERVICE_ACCOUNT_SOURCE)
        model.add(sourceNode, SchemaOrg.NAME, repository.valueFactory.createLiteral(sourceName))
        model.add(sourceNode, Personal.SOURCE_OF, accountNode)
        (ServiceAccountSource(
          service = serviceAccount.service,
          accountId = serviceAccount.accountId,
          sourceName = sourceName,
          iri = sourceNode), source)
    }
    (model, sources)
  }

  def convertService(statements: StatementSet, service: Service) = {
    val serviceNode = serviceIRI(service)
    statements.add(serviceNode, RDF.TYPE, Personal.SERVICE)
    statements.add(serviceNode, SchemaOrg.NAME, repository.valueFactory.createLiteral(service.name))
    statements
  }

  override def receive: Receive = {
    case serviceAccountTask: ServiceAccountSourceTask[_] =>
      val taskId = taskMap.get(serviceAccountTask.source).map(_._1).getOrElse {
        taskCounter += 1
        taskCounter
      }
      taskMap += serviceAccountTask.source -> (taskId, serviceAccountTask)
    case ListTasks =>
      sender() ! taskMap.values.toVector
    case AddServices(services) =>
      connection.begin()
      connection.add(services.foldLeft(StatementSet.empty(connection.getValueFactory)) {
        case (statements, service) => convertService(statements, service)
      }.asJavaCollection)
      connection.commit()
    case AddServiceAccount(serviceAccount) =>
      val (statements, sources) = serviceAccountModel(serviceAccount)
      connection.begin()
      connection.add(statements.asJavaCollection)
      connection.commit()
      pipeline.addServiceAccount(ServiceAccountSources(sources))
    case ApplyUpdate(update) =>
      implicit val timeout = com.thymeflow.actors.timeout
      implicit val ec = materializer.executionContext
      val s = sender()
      pipeline.applyUpdate(update).onComplete {
        t => s ! t
      }
  }

}


object Supervisor {

  def interactor(supervisor: ActorRef) = new Interactor(supervisor)

  class Interactor(supervisor: ActorRef) {
    def addServiceAccount(serviceAccount: ServiceAccount)(implicit sender: ActorRef = Actor.noSender): Unit = {
      supervisor ! AddServiceAccount(serviceAccount)
    }

    def listTasks()(implicit timeout: Timeout, sender: ActorRef = Actor.noSender) = {
      (supervisor ? ListTasks).asInstanceOf[Future[Seq[(Long, ServiceAccountSourceTask[TaskStatus])]]]
    }

    def applyUpdate(update: Update)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Try[UpdateResults]] = {
      (supervisor ? ApplyUpdate(update)).asInstanceOf[Future[Try[UpdateResults]]]
    }

    def addServices(services: Seq[Service]) = {
      supervisor ! AddServices(services)
    }
  }

  def props(repository: Repository,
            synchronizers: Seq[Synchronizer],
            enrichers: Graph[FlowShape[StatementSetDiff, StatementSetDiff], _])(implicit config: Config, materializer: Materializer) = Props(classOf[Supervisor], config, materializer, repository, synchronizers, enrichers)

  object ListTasks

  case class ApplyUpdate(update: Update)

  case class AddServiceAccount(serviceAccount: ServiceAccount)

  case class AddServices(services: Seq[Service])

}