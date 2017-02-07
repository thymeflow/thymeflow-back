package com.thymeflow.api

import java.io.OutputStream

import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{MissingFormFieldRejection, Route}
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.stream.scaladsl.{Keep, Sink, Source, StreamConverters}
import com.thymeflow.api.SparqlService.SparqlQuery
import com.thymeflow.rdf.model.StatementSet
import com.thymeflow.rdf.repository.Repository
import com.typesafe.scalalogging.StrictLogging
import org.eclipse.rdf4j.common.lang.FileFormat
import org.eclipse.rdf4j.common.lang.service.FileFormatServiceRegistry
import org.eclipse.rdf4j.model.vocabulary.{RDF, SD}
import org.eclipse.rdf4j.query._
import org.eclipse.rdf4j.query.parser.QueryParserUtil
import org.eclipse.rdf4j.query.resultio.{BooleanQueryResultWriterRegistry, TupleQueryResultWriterRegistry}
import org.eclipse.rdf4j.repository.RepositoryException
import org.eclipse.rdf4j.rio.{RDFWriterRegistry, Rio}

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future
import scala.language.implicitConversions

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
trait SparqlService extends StrictLogging with CorsSupport {
  val `application/sparql-query` = MediaType.applicationWithFixedCharset("sparql-query", HttpCharsets.`UTF-8`)
  implicit protected val sparqlQueryUnmarshaller = implicitly[FromEntityUnmarshaller[String]].map(SparqlQuery.apply).forContentTypes(`application/sparql-query`)
  protected val sparqlRoute = {
    corsHandler {
      optionalHeaderValueByType[Accept]() { accept =>
        get {
          parameter('query) { query =>
            executeQuery(query, accept)
          } ~
            executeDescription(accept)
        } ~
          post {
            // cannot use formField/formFields here due to an Akka issue
            // https://github.com/akka/akka/issues/19506
            entity(as[FormData]) { entity =>
              entity.fields.get("query") match {
                case Some(query) => executeQuery(query, accept)
                case None => entity.fields.get("update") match {
                  case Some(update) => executeUpdate(update, accept)
                  case None => reject(MissingFormFieldRejection("query"), MissingFormFieldRejection("update"))
                }
              }
            } ~
              entity(as[SparqlQuery]) { operation =>
                if (isSPARQLQuery(operation.content)) {
                  executeQuery(operation.content, accept)
                } else {
                  executeUpdate(operation.content, accept)
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

  private def executeQuery(queryStr: String, accept: Option[Accept]): Route = {
    try {
      val repositoryConnection = repository.newConnection()
      try {
        val query = repositoryConnection.prepareQuery(QueryLanguage.SPARQL, queryStr)
        executeQuery(query, accept) {
          () => repositoryConnection.close()
        }
      } catch {
        case e: MalformedQueryException =>
          repositoryConnection.close()
          complete(StatusCodes.BadRequest, "Malformed query: " + e.getMessage)
        case e: RepositoryException =>
          repositoryConnection.close()
          complete(StatusCodes.InternalServerError, s"Error preparing query: ${e.getMessage}")
      }
    } catch {
      case e: RepositoryException =>
        complete(StatusCodes.InternalServerError, s"Error retrieving repository connection: ${e.getMessage}")
    }
  }

  private def executeQuery(query: Query, accept: Option[Accept])(close: () => Unit): Route = {
    (query match {
      case query: BooleanQuery =>
        writerFactoryForAccept(BooleanQueryResultWriterRegistry.getInstance(), accept, "application/sparql-results+json").map {
          writerFactory =>
            (writerFactory.getBooleanQueryResultFormat, (os: OutputStream) => writerFactory.getWriter(os).handleBoolean(query.evaluate()))
        }
      case query: GraphQuery =>
        writerFactoryForAccept(RDFWriterRegistry.getInstance(), accept, "application/rdf+json").map {
          writerFactory =>
            (writerFactory.getRDFFormat, (os: OutputStream) => query.evaluate(writerFactory.getWriter(os)))
        }
      case query: TupleQuery =>
        writerFactoryForAccept(TupleQueryResultWriterRegistry.getInstance(), accept, "application/sparql-results+json").map {
          writerFactory =>
            (writerFactory.getTupleQueryResultFormat, (os: OutputStream) => query.evaluate(writerFactory.getWriter(os)))
        }
    }) match {
      case Some((format, f)) =>
        completeStream(format, os =>
          try {
            f(os)
          } catch {
            case e: QueryInterruptedException =>
              val message = s"Query timeout error: ${e.getMessage}"
              logger.error(message, e)
            case e: QueryEvaluationException =>
              val message = s"Query evaluation error: ${e.getMessage}"
              logger.error(message, e)
            case e: QueryResultHandlerException =>
              val message = s"Query result handler exception: ${e.getMessage}"
              logger.error(message, e)
            case e: Exception =>
              val message = s"Unexpected error during query evaluation."
              logger.error(message, e)
          } finally {
            close()
          }
        )
      case None => complete {
        StatusCodes.UnsupportedMediaType
      }
    }
  }

  private def executeUpdate(updateStr: String, accept: Option[Accept]): Route = {
    val repositoryConnection = repository.newConnection()
    try {
      repositoryConnection.prepareUpdate(QueryLanguage.SPARQL, updateStr).execute()
      complete(StatusCodes.OK, "")
    } catch {
      case e: MalformedQueryException => complete(StatusCodes.BadRequest, "Malformed query: " + e.getMessage)
      case e: UpdateExecutionException =>
        logger.error("Update execution error: " + e.getMessage, e)
        complete(StatusCodes.InternalServerError, "Update execution error: " + e.getMessage)
    } finally {
      repositoryConnection.close()
    }
  }

  private def executeDescription(accept: Option[Accept]): Route = {
    writerFactoryForAccept(RDFWriterRegistry.getInstance(), accept, "application/rdf+json") match {
      case Some(writerFactory) =>
        completeStream(writerFactory.getRDFFormat, os => Rio.write(sparqlServiceDescription.asJavaCollection, writerFactory.getWriter(os)))
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

  private def completeStream[V](format: FileFormat, f: OutputStream => V): Route = {
    import com.thymeflow.actors._
    val (out, pub) = StreamConverters.asOutputStream().toMat(Sink.asPublisher(false))(Keep.both).run()
    Future {
      try {
        f(out)
      } finally {
        out.close()
      }
    }
    val source = Source.fromPublisher(pub)
    complete(HttpEntity(contentTypeForFormat(format), source))
  }

  private def contentTypeForFormat(format: FileFormat): ContentType = {
    ContentType(MediaType.parse(format.getDefaultMIMEType).right.get, () => {
      HttpCharsets.getForKey(format.getCharset.name()).getOrElse(HttpCharsets.`UTF-8`)
    })
  }

  private def sparqlServiceDescription: StatementSet = {
    val valueFactory = repository.valueFactory
    val statements = StatementSet.empty(valueFactory)

    val service = valueFactory.createBNode()
    statements.add(service, RDF.TYPE, SD.SERVICE)
    //TODO model.add(service, SD.ENDPOINT, )
    statements.add(service, SD.FEATURE_PROPERTY, SD.UNION_DEFAULT_GRAPH)
    statements.add(service, SD.FEATURE_PROPERTY, SD.BASIC_FEDERATED_QUERY)
    statements.add(service, SD.SUPPORTED_LANGUAGE, SD.SPARQL_10_QUERY)
    statements.add(service, SD.SUPPORTED_LANGUAGE, SD.SPARQL_11_QUERY)

    TupleQueryResultWriterRegistry.getInstance().getAll.asScala.foreach(writer =>
      Option(writer.getTupleQueryResultFormat.getStandardURI).foreach(formatURI =>
        statements.add(service, SD.RESULT_FORMAT, formatURI)
      )
    )
    BooleanQueryResultWriterRegistry.getInstance().getAll.asScala.foreach(writer =>
      Option(writer.getBooleanQueryResultFormat.getStandardURI).foreach(formatURI =>
        statements.add(service, SD.RESULT_FORMAT, formatURI)
      )
    )
    RDFWriterRegistry.getInstance().getAll.asScala.foreach(writer =>
      Option(writer.getRDFFormat.getStandardURI).foreach(formatURI =>
        statements.add(service, SD.RESULT_FORMAT, formatURI)
      )
    )

    /*FunctionRegistry.getInstance().getKeys.asScala.foreach(sparqlFunction => {
      val functionURI = valueFactory.createIRI(sparqlFunction)
      model.add(functionURI, RDF.TYPE, SD.FUNCTION)
      model.add(service, SD.EXTENSION_FUNCTION, functionURI)
    }) TODO: remove not extension functions*/

    statements
  }

  private def isSPARQLQuery(operation: String): Boolean = {
    val operationPrefix = QueryParserUtil.removeSPARQLQueryProlog(operation).toUpperCase
    operationPrefix.startsWith("SELECT") ||
      operationPrefix.startsWith("CONSTRUCT") ||
      operationPrefix.startsWith("DESCRIBE") ||
      operationPrefix.startsWith("ASK")
  }
}


object SparqlService {

  case class SparqlQuery(content: String)

}