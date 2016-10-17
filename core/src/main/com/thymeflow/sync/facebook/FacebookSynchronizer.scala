package com.thymeflow.sync.facebook

import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.Source
import com.thymeflow.actors._
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.service.source.FacebookGraphApiSource
import com.thymeflow.service.{Idle, ServiceAccount, ServiceAccountSource, ServiceAccountSourceTask}
import com.thymeflow.sync.Synchronizer
import com.thymeflow.sync.publisher.ScrollDocumentPublisher
import com.typesafe.config.Config
import org.openrdf.model.{IRI, Model, ValueFactory}
import spray.json._

/**
  * @author David Montoya
  */
object FacebookSynchronizer extends Synchronizer with DefaultJsonProtocol {

  final val apiEndpoint = Uri("https://graph.facebook.com")
  final val namespace = "https://graph.facebook.com"
  final val apiPath = Path("/v2.6")

  def source(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory, supervisor)))

  sealed trait FacebookState

  case class Initial(token: String) extends FacebookState

  case class FacebookRequest(method: String, relative_url: String)

  case class Scroll(token: String, context: IRI, model: Model, eventIds: Vector[String]) extends FacebookState

  private class Publisher(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config)
    extends ScrollDocumentPublisher[Document, FacebookState] with BasePublisher with SprayJsonSupport {

    private val facebookConverter = new FacebookConverter(valueFactory)

    override def receive: Receive = super.receive orElse {
      case account: ServiceAccount =>
        account.sources.foreach {
          case (sourceName, source: FacebookGraphApiSource) =>
            val sourceId = ServiceAccountSource(account.service, account.accountId, sourceName)
            queue(Initial(source.accessToken))
            supervisor ! ServiceAccountSourceTask(source = sourceId, "Synchronization", Idle)
          case _ =>
        }
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
                  case events =>
                    events.foreach {
                      case (_, event) => facebookConverter.convert(event, scroll.model, scroll.context)
                    }
                }
            }.recover {
              case e =>
                logger.error(s"FacebookPublisher: error requesting event attendance $eventIds", e)
            }.map {
              _ =>
                if (tail.nonEmpty) {
                  Result(Some(scroll.copy(eventIds = tail)), Vector.empty)
                } else {
                  Result(None, Vector(Document(scroll.context, scroll.model)))
                }
            }
          case initial: Initial =>
            val queryMe = HttpRequest(HttpMethods.GET, apiEndpoint.withPath(apiPath / "me").withQuery(
              Query(
                ("access_token", initial.token),
                ("fields", "about,bio,age_range,email,first_name,last_name,gender,education,hometown,updated_time,events.limit(1000){id},taggable_friends.limit(1000)")
              )
            ))

            val f = Http().singleRequest(queryMe).flatMap {
              case HttpResponse(StatusCodes.OK, _, entity, _) =>
                Unmarshal(entity.withContentType(ContentTypes.`application/json`)).to[Me].map {
                  case me =>
                    val context = valueFactory.createIRI(s"http://graph.facebook.com")
                    val events = me.events.data.map(_.id)
                    val model = facebookConverter.convert(me, context)
                    Result(Some(Scroll(initial.token, context, model, events)), Vector.empty)
                }
            }
            f.recover {
              case e =>
                logger.error(s"FacebookPublisher: Error getting user data.", e)
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
