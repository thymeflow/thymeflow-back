package pkb.sync.dav

import java.net.SocketException
import javax.xml.namespace.QName

import com.github.sardine.impl.SardineException
import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.client.utils.URIBuilder
import org.openrdf.model.{IRI, Model, ValueFactory}
import pkb.actors._
import pkb.rdf.model.document.Document
import pkb.sync.Synchronizer
import pkb.utilities.ExceptionUtils

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

    private val fetchers = new mutable.ArrayBuffer[DocumentFetcher]()
    private val queue = new mutable.Queue[Document]

    system.scheduler.schedule(1 minute, 1 minute)({
      if (waitingForData) {
        fetchers.foreach(retrieveDocuments)
      }
    })

    protected def addFetcher(fetcher: DocumentFetcher): Unit = {
      fetchers += fetcher
      deliverWaitingDocuments()
      retrieveDocuments(fetcher)
    }

    protected def deliverWaitingDocuments(): Unit = {
      while (waitingForData && queue.nonEmpty) {
        onNext(queue.dequeue())
      }
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

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }
  }

  protected abstract class BaseDavDocumentsFetcher(valueFactory: ValueFactory, sardine: Sardine, baseUri: String) {

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
        case e: SardineException =>
          logger.error(ExceptionUtils.getUnrolledStackTrace(e))
          None
        case e: SocketException =>
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

    private def buildUriFromBaseAndPath(base: String, path: String): String = {
      new URIBuilder(base).setPath(path).toString
    }

    protected def dataNodeName: QName

    protected def buildQueryReport(withData: Boolean): SardineReport[Traversable[DavResource]]

    protected def buildMultigetReport(paths: Traversable[String]): SardineReport[Traversable[DavResource]]

    protected def convert(str: String, context: IRI): Model

    private def getDirectoryUris(base: String): Traversable[String] = {
      sardine.list(base.toString, 0).asScala.map(resource => buildUriFromBaseAndPath(base, resource.getPath))
    }
  }
}
