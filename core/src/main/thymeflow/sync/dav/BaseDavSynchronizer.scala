package thymeflow.sync.dav

import java.io.IOException
import javax.xml.namespace.QName

import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.client.utils.URIBuilder
import org.openrdf.model.{Model, Resource, ValueFactory}
import thymeflow.actors._
import thymeflow.rdf.model.document.Document
import thymeflow.sync.Synchronizer
import thymeflow.sync.dav.BaseDavSynchronizer._
import thymeflow.utilities.ExceptionUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  */
trait BaseDavSynchronizer extends Synchronizer with StrictLogging {

  protected abstract class BaseDavPublisher[DocumentFetcher <: BaseDavDocumentsFetcher](valueFactory: ValueFactory)
    extends BasePublisher {

    private val fetchers = new mutable.HashMap[String, DocumentFetcher]()
    private val queue = new mutable.Queue[Document]

    override def receive: Receive = {
      case Request(_) =>
        deliverWaitingDocuments()
      case Cancel =>
        context.stop(self)
      case Tick =>
        if (waitingForData) {
          fetchers.values.foreach(retrieveDocuments)
        }
        deliverWaitingDocuments()
    }

    system.scheduler.schedule(1 minute, 1 minute)({
      this.self ! Tick
    })

    protected def addFetcher(fetcher: DocumentFetcher): Unit = {
      //If it is a fetcher for the same base URI we only updates Sardine
      val newFetcher = fetchers.getOrElseUpdate(fetcher.baseUri, fetcher)
      newFetcher.updateSardine(fetcher.sardine)

      deliverWaitingDocuments()
      retrieveDocuments(newFetcher)
    }

    protected def deliverWaitingDocuments(): Unit = {
      while (waitingForData && queue.nonEmpty) {
        onNext(queue.dequeue())
      }
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    private def retrieveDocuments(fetcher: BaseDavDocumentsFetcher): Unit = {
      fetcher.newDocuments.foreach(document =>
        if (waitingForData) {
          onNext(document)
        } else {
          queue.enqueue(document)
        }
      )
    }
  }

  protected abstract class BaseDavDocumentsFetcher(valueFactory: ValueFactory, var sardine: Sardine, val baseUri: String) {

    private val elementsEtag = new mutable.HashMap[String, String]()
    private val paths = getDirectoryUris(baseUri)

    def newDocuments: Traversable[Document] = {
      paths.flatMap(newDocumentsFromDirectory)
    }

    private def newDocumentsFromDirectory(directoryUri: String): Traversable[Document] = {
      davResourcesOfUpdatedDocuments(directoryUri: String).flatMap(documentFromDavResource(_, directoryUri))
    }

    private def davResourcesOfUpdatedDocuments(directoryUri: String): Traversable[DavResource] = {
      try {
        if (elementsEtag.isEmpty) {
          //We load everything
          sardine.report(directoryUri, 1, buildQueryReport(true))
        } else {
          //We load only new documents
          val newPaths = sardine.report(directoryUri, 1, buildQueryReport(false)).filter(davResource =>
            !elementsEtag.get(davResource.getPath).contains(davResource.getEtag)
          ).map(davResource => davResource.getPath)
          if (newPaths.isEmpty) {
            None
          } else {
            sardine.report(directoryUri, 1, buildMultigetReport(newPaths))
          }
        }
      } catch {
        case e: IOException =>
          logger.error(ExceptionUtils.getUnrolledStackTrace(e))
          None
      }
    }

    private def documentFromDavResource(davResource: DavResource, directoryUri: String): Option[Document] = {
      elementsEtag.put(davResource.getPath, davResource.getEtag)
      Option(davResource.getCustomPropsNS.get(dataNodeName)).map(data => {
        val documentIri = valueFactory.createIRI(buildUriFromBaseAndPath(directoryUri, davResource.getPath))
        Document(documentIri, convert(data, documentIri))
      })
    }

    def updateSardine(newSardine: Sardine): Unit = {
      sardine = newSardine
    }

    protected def dataNodeName: QName

    protected def buildQueryReport(withData: Boolean): SardineReport[Traversable[DavResource]]

    protected def buildMultigetReport(paths: Traversable[String]): SardineReport[Traversable[DavResource]]

    protected def convert(str: String, context: Resource): Model

    private def getDirectoryUris(base: String): Traversable[String] = {
      sardine.list(base.toString, 0).asScala.map(resource => buildUriFromBaseAndPath(base, resource.getPath))
    }

    private def buildUriFromBaseAndPath(base: String, path: String): String = {
      new URIBuilder(base).setPath(path).toString
    }
  }
}

object BaseDavSynchronizer {

  object Tick

}