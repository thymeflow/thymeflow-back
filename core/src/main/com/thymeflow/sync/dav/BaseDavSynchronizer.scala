package com.thymeflow.sync.dav

import java.io.{ByteArrayInputStream, IOException}
import java.net.URI
import javax.xml.namespace.QName

import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import com.thymeflow.actors._
import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.sync.Synchronizer
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.sync.dav.BaseDavSynchronizer._
import com.thymeflow.update.UpdateResults
import com.thymeflow.utilities.ExceptionUtils
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.http.client.utils.URIBuilder
import org.openrdf.model.{Model, Resource, ValueFactory}

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
    private val fetcherForDirectory = new mutable.HashMap[String, DocumentFetcher]
    private val queue = new mutable.Queue[Document]

    override def receive: Receive = {
      case Request(_) =>
        deliverWaitingDocuments()
      case Cancel =>
        context.stop(self)
      case Tick =>
        if (waitingForData) {
          retrieveAllDocuments()
        }
        deliverWaitingDocuments()
      case Update(diff) =>
        retrieveAllDocuments() //Update state of sync
        sender() ! applyDiff(diff)
        retrieveAllDocuments()
        deliverWaitingDocuments()
    }

    system.scheduler.schedule(1 minute, 1 minute)({
      this.self ! Tick
    })

    protected def addFetcher(newFetcher: DocumentFetcher): Unit = {
      //If it is a fetcher for the same base URI we only updates Sardine
      val fetcher = fetchers.getOrElseUpdate(newFetcher.baseUri, newFetcher)
      fetcher.updateSardine(newFetcher.sardine)
      fetcher.directories.foreach(fetcherForDirectory.put(_, fetcher))

      deliverWaitingDocuments()
      retrieveDocuments(fetcher)
    }

    protected def deliverWaitingDocuments(): Unit = {
      while (waitingForData && queue.nonEmpty) {
        onNext(queue.dequeue())
      }
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    private def retrieveAllDocuments(): Unit = {
      fetchers.values.foreach(retrieveDocuments)
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

    private def applyDiff(diff: ModelDiff): UpdateResults = {
      UpdateResults.merge(diff.contexts().asScala.map(context => {
        val contextDiff = diff.filter(null, null, null, context)
        UpdateResults.merge(
          getFetchersForUri(context.toString)
            .map(_.applyDiff(contextDiff))
        )
      }))
    }

    private def getFetchersForUri(uri: String): Traversable[BaseDavDocumentsFetcher] = {
      fetcherForDirectory.filterKeys(uri.startsWith).values
    }
  }

  protected abstract class BaseDavDocumentsFetcher(valueFactory: ValueFactory, var sardine: Sardine, val baseUri: String) {

    protected val mimeType: String

    private val elementsEtag = new mutable.HashMap[String, String]()
    val directories = getDirectoryUris(baseUri)

    def newDocuments: Traversable[Document] = {
      directories.flatMap(newDocumentsFromDirectory)
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

    def applyDiff(diff: ModelDiff): UpdateResults = {
      UpdateResults.merge(diff.contexts().asScala.map(context => {
        val documentUrl = new URI(context.toString)
        val oldVersion = IOUtils.toString(sardine.get(documentUrl.toString))
        val (newVersion, result) = applyDiff(oldVersion, diff.filter(null, null, null, context))
        if (newVersion == oldVersion) {
          logger.info(s"No change for vCard $documentUrl")
          return result
        }

        var headers = Map(
          "Content-Type" -> mimeType
        )
        elementsEtag.get(documentUrl.getPath).foreach(etag =>
          headers += "If-Match" -> etag
        )
        sardine.put(documentUrl.toString, new ByteArrayInputStream(newVersion.getBytes), headers.asJava)
        result
      }))
    }

    protected def applyDiff(str: String, diff: ModelDiff): (String, UpdateResults)
  }
}

object BaseDavSynchronizer {

  object Tick

}