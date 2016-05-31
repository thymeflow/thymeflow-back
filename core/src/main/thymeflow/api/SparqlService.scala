package thymeflow.api

import java.io.ByteArrayOutputStream

import akka.http.scaladsl.model.MediaType._
import akka.http.scaladsl.model.headers.{Accept, `Access-Control-Allow-Methods`, `Access-Control-Allow-Origin`, `Content-Type`}
import akka.http.scaladsl.model.{ContentType, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route}
import com.typesafe.scalalogging.StrictLogging
import info.aduna.lang.FileFormat
import info.aduna.lang.service.FileFormatServiceRegistry
import org.openrdf.model.Model
import org.openrdf.model.vocabulary.{RDF, SD}
import org.openrdf.query._
import org.openrdf.query.resultio.{BooleanQueryResultWriterRegistry, TupleQueryResultWriterRegistry}
import org.openrdf.repository.Repository
import org.openrdf.rio.{RDFWriterRegistry, Rio}
import thymeflow.actors._
import thymeflow.rdf.model.SimpleHashModel

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

/**
  * @author Thomas Pellissier Tanon
  */
trait SparqlService extends StrictLogging {
  val `application/sparql-query` = applicationWithFixedCharset("sparql-query", HttpCharsets.`UTF-8`)
  protected val sparqlRoute = {
    respondWithHeaders(
      `Access-Control-Allow-Origin`.*,
      `Access-Control-Allow-Methods`(HttpMethods.GET, HttpMethods.POST, HttpMethods.OPTIONS)
    ) {
      optionalHeaderValueByType[Accept]() { accept =>
        get {
          parameter('query) { query =>
            execute(query, accept)
          } ~
            executeDescription(accept)
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
          } ~
          options {
            complete {
              StatusCodes.NoContent
            }
          }
      }
    }
  }

  protected def repository: Repository

  private def execute(queryStr: String, accept: Option[Accept]): Route = {
    val repositoryConnection = repository.getConnection
    try {
      repositoryConnection.prepareQuery(QueryLanguage.SPARQL, queryStr) match {
        case query: BooleanQuery => execute(query, accept)
        case query: GraphQuery => execute(query, accept)
        case query: TupleQuery => execute(query, accept)
      }
    } catch {
      case e: MalformedQueryException => complete(StatusCodes.BadRequest, "Malformed query: " + e.getMessage)
      case e: QueryInterruptedException => complete(StatusCodes.InternalServerError, "Query times out: " + e.getMessage)
      case e: QueryEvaluationException =>
        logger.error("Query evaluation error: " + e.getMessage, e)
        complete(StatusCodes.InternalServerError, "Query evaluation error: " + e.getMessage)
    } finally {
      repositoryConnection.close()
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

  private def executeDescription(accept: Option[Accept]): Route = {
    writerFactoryForAccept(RDFWriterRegistry.getInstance(), accept, "application/rdf+json") match {
      case Some(writerFactory) =>
        val outputStream = new ByteArrayOutputStream()
        Rio.write(sparqlServiceDescription, writerFactory.getWriter(outputStream))
        completeStream(outputStream, writerFactory.getRDFFormat)
      case None => complete {
        StatusCodes.UnsupportedMediaType
      }
    }
  }

  private def writerFactoryForAccept[FF <: FileFormat, S](writerRegistry: FileFormatServiceRegistry[FF, S], accept: Option[Accept], defaultMimeType: String): Option[S] = {
    val acceptedMimeTypes = accept
      .map(_.mediaRanges.map(_.value.split(";")(0)))
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
    ContentType(MediaType.parse(format.getDefaultMIMEType).right.get, () => {
      HttpCharsets.getForKey(format.getCharset.name()).getOrElse(HttpCharsets.`UTF-8`)
    })
  }

  private def withContentType(expectedContentType: ContentType): Directive0 = {
    optionalHeaderValueByType[`Content-Type`]().require({
      case Some(contentType) if contentType.contentType.mediaType.equals(expectedContentType.mediaType) => true
      case _ => false
    })
  }

  private def sparqlServiceDescription: Model = {
    val valueFactory = repository.getValueFactory
    val model = new SimpleHashModel(valueFactory)

    val service = valueFactory.createBNode()
    model.add(service, RDF.TYPE, SD.SERVICE)
    //TODO model.add(service, SD.ENDPOINT, )
    model.add(service, SD.FEATURE_PROPERTY, SD.UNION_DEFAULT_GRAPH)
    model.add(service, SD.FEATURE_PROPERTY, SD.BASIC_FEDERATED_QUERY)
    model.add(service, SD.SUPPORTED_LANGUAGE, SD.SPARQL_10_QUERY)
    model.add(service, SD.SUPPORTED_LANGUAGE, SD.SPARQL_11_QUERY)

    TupleQueryResultWriterRegistry.getInstance().getAll.asScala.foreach(writer =>
      Option(writer.getTupleQueryResultFormat.getStandardURI).foreach(formatURI =>
        model.add(service, SD.RESULT_FORMAT, formatURI)
      )
    )
    BooleanQueryResultWriterRegistry.getInstance().getAll.asScala.foreach(writer =>
      Option(writer.getBooleanQueryResultFormat.getStandardURI).foreach(formatURI =>
        model.add(service, SD.RESULT_FORMAT, formatURI)
      )
    )
    RDFWriterRegistry.getInstance().getAll.asScala.foreach(writer =>
      Option(writer.getRDFFormat.getStandardURI).foreach(formatURI =>
        model.add(service, SD.RESULT_FORMAT, formatURI)
      )
    )

    /*FunctionRegistry.getInstance().getKeys.asScala.foreach(sparqlFunction => {
      val functionURI = valueFactory.createIRI(sparqlFunction)
      model.add(functionURI, RDF.TYPE, SD.FUNCTION)
      model.add(service, SD.EXTENSION_FUNCTION, functionURI)
    }) TODO: remove not extension functions*/

    model
  }
}
