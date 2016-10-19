package com.thymeflow.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.thymeflow.api.JsonApi.{ResourceObject, ResourceObjects}
import com.thymeflow.api.ServicesService.{Account, JsonProtocol, Service}
import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.rdf.repository.Repository
import org.openrdf.model.{IRI, Literal}
import org.openrdf.query.QueryLanguage
import spray.json.{JsonFormat, RootJsonFormat}

import scala.language.{implicitConversions, postfixOps}

/**
  * @author David Montoya
  */
trait ServicesService extends Directives with CorsSupport {
  protected def repository: Repository

  import JsonProtocol._
  import SprayJsonSupport._

  private val servicesQuery =
    s"""
  SELECT ?service (SAMPLE(?serviceName) AS ?serviceName) ?account (SAMPLE(?accountName) as ?accountName) (SUM(?sourceMessagesN) as ?messagesN) (SUM(?sourceLocationN) as ?locationsN) (SUM(?sourceEventsN) as ?eventsN) (SUM(?sourceAgentsN) as ?agentsN)
  WHERE {
    ?service a <${Personal.SERVICE}> ;
      <${SchemaOrg.NAME}> ?serviceName .
    OPTIONAL {
      ?account <${Personal.ACCOUNT_OF}> ?service ;
        <${SchemaOrg.NAME}> ?accountName .

      ?source <${Personal.SOURCE_OF}> ?account .
      {
        SELECT ?source (COUNT(?message) as ?sourceMessagesN)
        WHERE {
          ?source a <${Personal.SERVICE_ACCOUNT_SOURCE}> .
          OPTIONAL{
            ?document <${Personal.DOCUMENT_OF}> ?source .
            GRAPH ?document {
              ?message a <${SchemaOrg.EMAIL_MESSAGE}> .
            }
          }
        }
        GROUP BY ?source
      }
      {
        SELECT ?source (COUNT(?location) as ?sourceLocationN)
        WHERE {
          ?source a <${Personal.SERVICE_ACCOUNT_SOURCE}> .
          OPTIONAL{
            ?document <${Personal.DOCUMENT_OF}> ?source .
            GRAPH ?document {
              ?location a <${Personal.LOCATION}> .
            }
          }
        }
        GROUP BY ?source
      }
      {
        SELECT ?source (COUNT(?event) as ?sourceEventsN)
        WHERE {
          ?source a <${Personal.SERVICE_ACCOUNT_SOURCE}> .
          OPTIONAL{
            ?document <${Personal.DOCUMENT_OF}> ?source .
            GRAPH ?document {
             ?event a <${SchemaOrg.EVENT}> .
            }
          }
        }
        GROUP BY ?source
      }
      {
        SELECT ?source (COUNT(?agent) as ?sourceAgentsN)
        WHERE {
          ?source a <${Personal.SERVICE_ACCOUNT_SOURCE}> .
          OPTIONAL {
            ?document <${Personal.DOCUMENT_OF}> ?source .
            GRAPH ?document {
              ?agent a <${Personal.AGENT}> .
            }
          }
        }
        GROUP BY ?source
      }
    }
  }
  GROUP BY ?service ?account
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
              Option(bindingSet.getValue("eventsN")),
              Option(bindingSet.getValue("messagesN")),
              Option(bindingSet.getValue("locationsN")),
              Option(bindingSet.getValue("agentsN")))
          ).toVector
          val result = ResourceObjects(

            v.collect {
              case (Some(service: IRI), Some(serviceName: Literal), account, accountName, Some(eventsN: Literal), Some(messagesN: Literal), Some(locationsN: Literal), Some(agentsN: Literal)) =>
                ((service, serviceName), account, accountName, eventsN, messagesN, locationsN, agentsN)
            }.groupBy(_._1).map {
              case ((service, serviceName), g) =>
                val accounts = g.collect {
                  case (_, Some(account: IRI), Some(accountName: Literal), eventsN, messagesN, locationsN, agentsN) =>
                    Account(name = accountName.stringValue(),
                      eventsCount = eventsN.longValue(),
                      locationsCount = locationsN.longValue(),
                      messagesCount = messagesN.longValue(),
                      agentsCount = agentsN.longValue())
                }
                ResourceObject(
                  id = Some(service.stringValue()),
                  `type` = "service",
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

object ServicesService {

  case class Account(name: String, eventsCount: Long, locationsCount: Long, messagesCount: Long, agentsCount: Long)

  case class Service(name: String, accounts: Seq[Account], eventsCount: Long, locationsCount: Long, messagesCount: Long, agentsCount: Long)

  object JsonProtocol extends JsonApi.JsonProtocol {
    implicit val accountFormat: JsonFormat[Account] = jsonFormat5(Account)
    implicit val serviceFormat: RootJsonFormat[Service] = jsonFormat6(Service)
  }

}
