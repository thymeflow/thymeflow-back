package pkb.api

import java.io.ByteArrayOutputStream
import java.util.Properties
import javax.mail.Session

import akka.actor.ActorSystem
import com.github.sardine.impl.SardineImpl
import info.aduna.lang.FileFormat
import info.aduna.lang.service.FileFormatServiceRegistry
import org.apache.commons.io.IOUtils
import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.query._
import org.openrdf.query.resultio.{BooleanQueryResultWriterRegistry, TupleQueryResultWriterRegistry}
import org.openrdf.rio.RDFWriterRegistry
import pkb.Pipeline
import pkb.rdf.RepositoryFactory
import pkb.sync.{CalDavSynchronizer, CardDavSynchronizer, EmailSynchronizer}
import spray.http.HttpHeaders.{Accept, `Content-Type`}
import spray.http._
import spray.routing._

import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * @author Thomas Pellissier Tanon
  */
object MainApi extends App with SimpleRoutingApp {

  implicit val system = ActorSystem("pkb")
  val `application/sparql-query` = MediaTypes.register(MediaType.custom("application", "sparql-query", compressible = true))

  private val repositoryConnection = RepositoryFactory.initializedMemoryRepository.getConnection
  private val pipeline = new Pipeline(repositoryConnection)

  startServer(interface = "localhost", port = 8080) {
    path("sparql") {
      optionalHeaderValueByType[Accept]() { accept =>
        get {
          parameter('query) { query =>
            execute(query, accept)
          }
        } ~
          post {
            formField('query) { query =>
              execute(query, accept)
            } ~
              withContentType(`application/sparql-query`) {
                entity(as[String]) { query =>
                  execute(query, accept)
                }
              }
          }
      }
    } ~
      pathPrefix("oauth") {
        pathPrefix("google") {
          path("auth") {
            redirect(OAuth2.Google.getAuthUri(Array(
              "https://www.googleapis.com/auth/userinfo.email",
              "https://www.googleapis.com/auth/carddav",
              "https://www.googleapis.com/auth/calendar",
              "https://mail.google.com/"
            ), "http://localhost:8080/oauth/google/token"), StatusCodes.TemporaryRedirect) //TODO: avoid to hardcode the URI
          } ~
            path("token") {
              parameter('code) { code =>
                OAuth2.Google.getAccessToken(code, "http://localhost:8080/oauth/google/token").foreach(onGoogleToken)
                complete {
                  StatusCodes.OK
                }
              }
            }
        }
    }
  }

  private def execute(queryStr: String, accept: Option[Accept]): Route = {
    try {
      repositoryConnection.prepareQuery(QueryLanguage.SPARQL, queryStr) match {
        case query: BooleanQuery => execute(query, accept)
        case query: GraphQuery => execute(query, accept)
        case query: TupleQuery => execute(query, accept)
      }
    } catch {
      case e: MalformedQueryException => complete(StatusCodes.BadRequest, "Malformated query: " + e.getMessage)
      case e: QueryInterruptedException => complete(StatusCodes.InternalServerError, "Query times out: " + e.getMessage)
      case e: QueryEvaluationException => complete(StatusCodes.InternalServerError, "Query evaluation error: " + e.getMessage)
    }
  }

  private def execute(query: BooleanQuery, accept: Option[Accept]): Route = {
    writerFactoryForAccept(BooleanQueryResultWriterRegistry.getInstance(), accept, "application/sparql-results+json") match {
      case Some(writerFactory) =>
        val outputStream = new ByteArrayOutputStream()
        writerFactory.getWriter(outputStream).handleBoolean(query.evaluate())
        completeStream(outputStream, writerFactory.getBooleanQueryResultFormat)
      case None => complete {
        StatusCodes.UnsupportedMediaType
      }
    }
  }

  private def execute(query: GraphQuery, accept: Option[Accept]): Route = {
    writerFactoryForAccept(RDFWriterRegistry.getInstance(), accept, "application/rdf+json") match {
      case Some(writerFactory) =>
        val outputStream = new ByteArrayOutputStream()
        query.evaluate(writerFactory.getWriter(outputStream))
        completeStream(outputStream, writerFactory.getRDFFormat)
      case None => complete {
        StatusCodes.UnsupportedMediaType
      }
    }
  }

  private def execute(query: TupleQuery, accept: Option[Accept]): Route = {
    writerFactoryForAccept(TupleQueryResultWriterRegistry.getInstance(), accept, "application/sparql-results+json") match {
      case Some(writerFactory) =>
        val outputStream = new ByteArrayOutputStream()
        query.evaluate(writerFactory.getWriter(outputStream))
        completeStream(outputStream, writerFactory.getTupleQueryResultFormat)
      case None => complete {
        StatusCodes.UnsupportedMediaType
      }
    }
  }

  private def writerFactoryForAccept[FF <: FileFormat, S](writerRegistry: FileFormatServiceRegistry[FF, S], accept: Option[Accept], defaultMimeType: String): Option[S] = {
    val acceptedMimeTypes = accept
      .map(_.mediaRanges.map(_.value))
      .getOrElse(Seq(defaultMimeType))
      .map(mimeType => if (mimeType == "*/*") {
        defaultMimeType
      } else {
        mimeType
      })
    acceptedMimeTypes.flatMap(writerRegistry.getFileFormatForMIMEType(_).asScala.flatMap(writerRegistry.get(_).asScala))
      .headOption
  }

  private def completeStream(outputStream: ByteArrayOutputStream, format: FileFormat): Route = {
    val byteArray = outputStream.toByteArray
    outputStream.close()
    complete(HttpEntity(contentTypeForFormat(format), byteArray))
  }

  private def contentTypeForFormat(format: FileFormat): ContentType = {
    val mediaType = MediaType.custom(format.getDefaultMIMEType)
    MediaTypes.register(mediaType)
    ContentType(mediaType, HttpCharsets.getForKey(format.getCharset.name()))
  }

  private def withContentType(expectedContentType: ContentType): Directive0 = {
    optionalHeaderValueByType[`Content-Type`]().require({
      case Some(contentType) if contentType.contentType.mediaType.equals(expectedContentType.mediaType) => true
      case _ => false
    })
  }

  private def onGoogleToken(token: String): Unit = {
    val sardine = new SardineImpl(token)

    //Get user email
    val gmailAddress = IOUtils.toString(sardine.get("https://www.googleapis.com/userinfo/email"))
      .split("&")(0).split("=")(1) //The result has the format "email=foo@gmail.com&..."

    //CardDav
    pipeline.addSynchronizer(
      new CardDavSynchronizer(SimpleValueFactory.getInstance, sardine, "https://www.googleapis.com/.well-known/carddav")
    )

    //CalDav
    pipeline.addSynchronizer(
      new CalDavSynchronizer(SimpleValueFactory.getInstance, sardine, "https://apidata.googleusercontent.com/caldav/v2/" + gmailAddress + "/events/")
    )

    //Emails
    val props = new Properties()
    props.put("mail.imap.ssl.enable", "true")
    props.put("mail.imap.auth.mechanisms", "XOAUTH2")
    val store = Session.getInstance(props).getStore("imap")
    store.connect("imap.gmail.com", gmailAddress, token)
    pipeline.addSynchronizer(new EmailSynchronizer(SimpleValueFactory.getInstance, store, 100))

    pipeline.run(2) //TODO: bad hack
  }
}
