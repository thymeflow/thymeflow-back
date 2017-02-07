package com.thymeflow.sync

import javax.xml.namespace.QName

import akka.actor.{ActorRef, Props}
import akka.stream.scaladsl.Source
import com.github.sardine.DavResource
import com.github.sardine.report.SardineReport
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import com.thymeflow.service.source.{CalDavSource, DavSource}
import com.thymeflow.service.{ServiceAccountSourceTask, TaskStatus}
import com.thymeflow.sync.converter.ICalConverter
import com.thymeflow.sync.dav.{BaseDavSynchronizer, CalendarMultigetReport, CalendarQueryReport}
import com.thymeflow.update.UpdateResults
import com.typesafe.config.Config
import org.eclipse.rdf4j.model.{Resource, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
object CalDavSynchronizer extends BaseDavSynchronizer {

  def source(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory, supervisor)))

  private class Publisher(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config)
    extends BaseDavPublisher[DocumentsFetcher](valueFactory, supervisor) {
    override def newFetcher(source: DavSource, task: ServiceAccountSourceTask[TaskStatus]): DocumentsFetcher = new DocumentsFetcher(supervisor, valueFactory, task, source)

    override protected def isValidSource(davSource: DavSource): Boolean = davSource.isInstanceOf[CalDavSource]
  }

  private class DocumentsFetcher(supervisor: ActorRef, valueFactory: ValueFactory, task: ServiceAccountSourceTask[TaskStatus], source: DavSource)(implicit config: Config)
    extends BaseDavDocumentsFetcher(supervisor, valueFactory, task, source) {

    private val CalDavNamespace = "urn:ietf:params:xml:ns:caldav"
    override protected val mimeType = "text/calendar"
    private val iCalConverter = new ICalConverter(valueFactory)

    override protected def dataNodeName = new QName(CalDavNamespace, "calendar-data")

    override protected def buildQueryReport(withData: Boolean): SardineReport[Traversable[DavResource]] = {
      new CalendarQueryReport(withData)
    }

    override protected def buildMultigetReport(paths: Traversable[String]): SardineReport[Traversable[DavResource]] = {
      new CalendarMultigetReport(paths)
    }

    override protected def convert(str: String, context: Resource): StatementSet = {
      iCalConverter.convert(str, context)
    }

    override def applyDiff(str: String, diff: StatementSetDiff): (String, UpdateResults) = {
      iCalConverter.applyDiff(str, diff)
    }
  }
}
