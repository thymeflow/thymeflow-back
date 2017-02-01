package com.thymeflow.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.thymeflow.api.DataServicesService.{Account, JsonProtocol, Service}
import com.thymeflow.api.JsonApi.{ResourceObject, ResourceObjects}
import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.rdf.repository.Repository
import org.eclipse.rdf4j.model.{IRI, Literal}
import org.eclipse.rdf4j.query.QueryLanguage
import spray.json.{JsonFormat, RootJsonFormat}

import scala.language.{implicitConversions, postfixOps}

/**
  * @author David Montoya
  */
trait DataServicesService extends Directives with CorsSupport {
  protected def repository: Repository

  import JsonProtocol._
  import SprayJsonSupport._

  private val servicesQuery =
    s"""
SELECT ?service (SAMPLE(?serviceName) AS ?serviceName) ?account (SAMPLE(?accountName) as ?accountName) ?type (COUNT(?instance) AS ?count)
WHERE {
  ?service a <${Personal.SERVICE}> ;
    <${SchemaOrg.NAME}> ?serviceName .

  OPTIONAL {
    ?account <${Personal.ACCOUNT_OF}> ?service ;
      <${SchemaOrg.NAME}> ?accountName .

    ?source <${Personal.SOURCE_OF}> ?account .

    OPTIONAL {
      ?document <${Personal.DOCUMENT_OF}> ?source .
      GRAPH ?document {
        ?instance a ?type .
        FILTER(?type = <${SchemaOrg.EVENT}> || ?type = <${Personal.LOCATION}> || ?type = <${SchemaOrg.EMAIL_MESSAGE}> || ?type = <${Personal.AGENT}> )
      }
    }
  }
}
GROUP BY ?service ?account ?type
"""


  protected val servicesRoute = {
    corsHandler {
      get {
        complete {
          val repositoryConnection = repository.newConnection()
          val preparedQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, servicesQuery)
          val v = preparedQuery.evaluate().toIterator.map(bindingSet =>
            (Option(bindingSet.getValue("service")),
              Option(bindingSet.getValue("serviceName")),
              Option(bindingSet.getValue("account")),
              Option(bindingSet.getValue("accountName")),
              Option(bindingSet.getValue("type")),
              Option(bindingSet.getValue("count")))
          ).toVector
          val result = ResourceObjects(

            v.collect {
              case (Some(service: IRI), Some(serviceName: Literal), account, accountName, instanceType, Some(count: Literal)) =>
                ((service, serviceName), account, accountName, instanceType, count)
            }.groupBy(_._1).map {
              case ((service, serviceName), g) =>
                val accounts = g.collect {
                  case (_, Some(account: IRI), Some(accountName: Literal), instanceType, count) =>
                    (account, accountName, instanceType, count)
                }.groupBy(_._1).map {
                  case (account, h) =>
                    val eventsCount = h.collectFirst {
                      case (_, _, Some(SchemaOrg.EVENT), count) => count.longValue()
                    }.getOrElse(0L)
                    val agentsCount = h.collectFirst {
                      case (_, _, Some(Personal.AGENT), count) => count.longValue()
                    }.getOrElse(0L)
                    val messagesCount = h.collectFirst {
                      case (_, _, Some(SchemaOrg.EMAIL_MESSAGE), count) => count.longValue()
                    }.getOrElse(0L)
                    val locationsCount = h.collectFirst {
                      case (_, _, Some(Personal.LOCATION), count) => count.longValue()
                    }.getOrElse(0L)
                    Account(h.head._2.stringValue(),
                      eventsCount = eventsCount,
                      messagesCount = messagesCount,
                      locationsCount = locationsCount,
                      agentsCount = agentsCount)
                }.toVector
                ResourceObject(
                  id = Some(service.stringValue()),
                  `type` = "data-service",
                  Service(name = serviceName.stringValue(),
                    eventsCount = accounts.map(_.eventsCount).sum,
                    locationsCount = accounts.map(_.locationsCount).sum,
                    messagesCount = accounts.map(_.messagesCount).sum,
                    agentsCount = accounts.map(_.agentsCount).sum,
                    accounts = accounts
                  )
                )
            }.toVector
          )
          repositoryConnection.close()
          result
        }
      }
    }
  }
}

object DataServicesService {

  case class Account(name: String, eventsCount: Long, locationsCount: Long, messagesCount: Long, agentsCount: Long)

  case class Service(name: String, accounts: Seq[Account], eventsCount: Long, locationsCount: Long, messagesCount: Long, agentsCount: Long)

  object JsonProtocol extends JsonApi.JsonProtocol {
    implicit val accountFormat: JsonFormat[Account] = jsonFormat5(Account)
    implicit val serviceFormat: RootJsonFormat[Service] = jsonFormat6(Service)
  }

}
