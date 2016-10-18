package com.thymeflow

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import akka.stream.{SourceShape, _}
import akka.util.Timeout
import com.thymeflow.Supervisor.{AddServiceAccount, ApplyUpdate, ListTasks}
import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.rdf.repository.Repository
import com.thymeflow.service.{ServiceAccount, ServiceAccountSource, ServiceAccountSourceTask, TaskStatus}
import com.thymeflow.sync.Synchronizer
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.update.UpdateResults
import com.thymeflow.utilities.VectorExtensions
import com.typesafe.config.Config

import scala.concurrent.Future

/**
  * @author David Montoya
  */
class Supervisor(config: Config,
                 materializer: Materializer,
                 repository: Repository,
                 synchronizers: Seq[Synchronizer],
                 enrichers: Graph[FlowShape[ModelDiff, ModelDiff], _]) extends Actor {
  protected var taskCounter = 0L
  protected val taskMap = new scala.collection.mutable.HashMap[ServiceAccountSource, (Long, ServiceAccountSourceTask[_])]

  require(synchronizers.nonEmpty, "Supervisor requires at least one synchronizer")

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

  override def receive: Receive = {
    case serviceAccountTask: ServiceAccountSourceTask[_] =>
      val taskId = taskMap.get(serviceAccountTask.source).map(_._1).getOrElse {
        taskCounter += 1
        taskCounter
      }
      taskMap += serviceAccountTask.source -> (taskId, serviceAccountTask)
    case ListTasks =>
      sender() ! taskMap.values.toVector
    case AddServiceAccount(serviceAccount) =>
      pipeline.addServiceAccount(serviceAccount)
    case ApplyUpdate(update) =>
      implicit val timeout = com.thymeflow.actors.timeout
      sender() ! pipeline.applyUpdate(update)
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

    def applyUpdate(update: Update)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[UpdateResults] = {
      (supervisor ? ApplyUpdate(update)).asInstanceOf[Future[UpdateResults]]
    }
  }

  def props(repository: Repository,
            synchronizers: Seq[Synchronizer],
            enrichers: Graph[FlowShape[ModelDiff, ModelDiff], _])(implicit config: Config, materializer: Materializer) = Props(classOf[Supervisor], config, materializer, repository, synchronizers, enrichers)

  object ListTasks

  case class ApplyUpdate(update: Update)

  case class AddServiceAccount(serviceAccount: ServiceAccount)

}