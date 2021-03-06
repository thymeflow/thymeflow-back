package com.thymeflow.sync

import javax.xml.namespace.QName

import akka.actor.{ActorRef, Props}
import akka.stream.scaladsl.Source
import com.github.sardine.DavResource
import com.github.sardine.report.SardineReport
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import com.thymeflow.service.source.{CardDavSource, DavSource}
import com.thymeflow.service.{ServiceAccountSourceTask, TaskStatus}
import com.thymeflow.sync.converter.VCardConverter
import com.thymeflow.sync.dav._
import com.thymeflow.update.UpdateResults
import com.typesafe.config.Config
import org.eclipse.rdf4j.model.{Resource, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
object CardDavSynchronizer extends BaseDavSynchronizer {

  def source(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory, supervisor)))

  private class Publisher(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config)
    extends BaseDavPublisher[DocumentsFetcher](valueFactory, supervisor) {
    override def newFetcher(source: DavSource, task: ServiceAccountSourceTask[TaskStatus]): DocumentsFetcher = new DocumentsFetcher(supervisor, valueFactory, task, source)

    override protected def isValidSource(davSource: DavSource): Boolean = davSource.isInstanceOf[CardDavSource]
  }

  private class DocumentsFetcher(supervisor: ActorRef, valueFactory: ValueFactory, task: ServiceAccountSourceTask[TaskStatus], source: DavSource)(implicit config: Config)
    extends BaseDavDocumentsFetcher(supervisor, valueFactory, task, source) {

    private val CardDavNamespace = "urn:ietf:params:xml:ns:carddav"
    override protected val mimeType = "text/vcard"
    private val vCardConverter = new VCardConverter(valueFactory)

    override protected def dataNodeName = new QName(CardDavNamespace, "address-data")

    override protected def buildQueryReport(withData: Boolean): SardineReport[Traversable[DavResource]] = {
      new AddressbookQueryReport(withData)
    }

    override protected def buildMultigetReport(paths: Traversable[String]): SardineReport[Traversable[DavResource]] = {
      new AddressbookMultigetReport(paths)
    }

    override protected def convert(str: String, context: Resource): StatementSet = {
      vCardConverter.convert(str, context)
    }

    override def applyDiff(str: String, diff: StatementSetDiff): (String, UpdateResults) = {
      vCardConverter.applyDiff(str, diff)
    }
  }
}
