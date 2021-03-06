package com.thymeflow.sync.facebook

import java.time.Instant

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.Source
import com.thymeflow.actors._
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.rdf.model.vocabulary.Personal
import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import com.thymeflow.service._
import com.thymeflow.service.source.FacebookGraphApiSource
import com.thymeflow.sync.Synchronizer
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.sync.converter.ConverterException
import com.thymeflow.sync.publisher.ScrollDocumentPublisher
import com.thymeflow.update.UpdateResults
import com.typesafe.config.Config
import org.eclipse.rdf4j.model.{IRI, ValueFactory}
import spray.json._


/**
  * @author David Montoya
  */
object FacebookSynchronizer extends Synchronizer with DefaultJsonProtocol {

  import Facebook.{apiEndpoint, apiPath, namespace}

  def source(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory, supervisor)))

  case class FacebookRequest(method: String, relative_url: String)

  sealed trait FacebookState

  case class Initial(task: ServiceAccountSourceTask[TaskStatus], token: String) extends FacebookState

  case class Scroll(task: ServiceAccountSourceTask[Working], token: String, context: IRI, statements: StatementSet, eventIds: Vector[String]) extends FacebookState

  private class Publisher(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config)
    extends ScrollDocumentPublisher[Document, FacebookState] with BasePublisher with SprayJsonSupport {

    private val facebookConverter = new FacebookConverter(valueFactory)

    override def receive: Receive = super.receive orElse {
      case serviceAccountSources: ServiceAccountSources =>
        serviceAccountSources.sources.foreach {
          case (serviceAccountSource, source: FacebookGraphApiSource) =>
            val task = ServiceAccountSourceTask(source = serviceAccountSource, "Synchronization", Idle)
            queue(Initial(task, source.accessToken))
            supervisor ! task
          case _ =>
        }
      case Update(diff) => sender() ! applyDiff(diff)
    }

    private def applyDiff(diff: StatementSetDiff): UpdateResults = {
      val failedContexts = diff.contexts().filter(_.stringValue().startsWith(namespace))
      UpdateResults.allFailed(
        diff.filter(statement => failedContexts.contains(statement.getContext)),
        new ConverterException("Facebook modification is not yet supported.")
      )
    }

    override protected def queryBuilder = {
      case (queuedState, demand) =>
        queuedState match {
          case scroll: Scroll =>
            val (eventIds, tail) = scroll.eventIds.splitAt(10)
            val request = queryAttending(scroll.token, eventIds)
            Http().singleRequest(request).flatMap {
              case HttpResponse(StatusCodes.OK, _, entity, _) =>
                Unmarshal(entity.withContentType(ContentTypes.`application/json`)).to[Map[String, Event]].map {
                  events =>
                    events.foreach {
                      case (_, event) => facebookConverter.convert(event, scroll.statements, scroll.context)
                    }
                }
            }.recover {
              case e =>
                logger.error(s"FacebookPublisher: error requesting event attendance $eventIds", e)
            }.map {
              _ =>
                if (tail.nonEmpty) {
                  val resultTask = scroll.task.status match {
                    case Working(_, Some(progress)) =>
                      scroll.task.copy(status = scroll.task.status.copy(progress = Some(progress.copy(value = progress.value + eventIds.size))))
                    case _ =>
                      scroll.task
                  }
                  supervisor ! resultTask
                  Result(Some(scroll.copy(eventIds = tail)), Vector.empty)
                } else {
                  val resultTask = scroll.task.status match {
                    case Working(startDate, Some(progress)) =>
                      scroll.task.copy(status = Done(startDate = startDate, endDate = Instant.now()))
                    case _ =>
                      scroll.task
                  }
                  scroll.statements.add(scroll.context, Personal.DOCUMENT_OF, scroll.task.source.iri, scroll.context)
                  supervisor ! resultTask
                  Result(None, Vector(Document(scroll.context, scroll.statements)))
                }
            }
          case initial: Initial =>
            val queryMe = HttpRequest(HttpMethods.GET, apiEndpoint.withPath(apiPath / "me").withQuery(
              Query(
                ("access_token", initial.token),
                ("fields", "about,age_range,email,first_name,last_name,gender,education,hometown,updated_time,events.limit(1000){id},taggable_friends.limit(1000)")
              )
            ))
            val startDate = Instant.now
            val initialTaskStatus = Working(startDate = startDate, progress = None)
            val initialTask = initial.task.copy(status = initialTaskStatus)
            supervisor ! initialTask
            val f = Http().singleRequest(queryMe).flatMap {
              case HttpResponse(StatusCodes.OK, _, entity, _) =>
                Unmarshal(entity.withContentType(ContentTypes.`application/json`)).to[Me].map {
                  me =>
                    val context = valueFactory.createIRI(namespace)
                    val events = me.events.data.map(_.id)
                    val statements = facebookConverter.convert(me, context)
                    val resultTask = initialTask.copy(status = initialTaskStatus.copy(progress = Some(Progress(value = 0L, total = events.size))))
                    supervisor ! resultTask
                    Result(Some(Scroll(resultTask, initial.token, context, statements, events)), Vector.empty)
                }
            }
            f.recover {
              case e =>
                logger.error(s"FacebookPublisher: Error getting user data.", e)
                initial.task.copy(status = initialTaskStatus)
                supervisor ! initialTask.copy(status = Error(startDate = initialTaskStatus.startDate, errorDate = Instant.now()))
                Result(None, Vector.empty)
            }
        }
    }

    def queryAttending(token: String, eventIds: Vector[String]) = {
      HttpRequest(HttpMethods.GET, apiEndpoint.withPath(apiPath / "").withQuery(
        Query(
          ("access_token", token),
          ("fields", "id,attending_count,can_guests_invite,category,cover,declined_count,description,end_time,guest_list_enabled,interested_count,is_page_owned,is_viewer_admin,maybe_count,name,noreply_count,owner,parent_group,place,start_time,ticket_uri,timezone,type,updated_time,attending.limit(100),declined.limit(100),interested.limit(100),maybe.limit(100),noreply.limit(100)"),
          ("ids", eventIds.mkString(","))
        )
      ))
    }

  }

}
