package com.thymeflow.sync

import javax.xml.namespace.QName

import akka.actor.Props
import akka.stream.scaladsl.Source
import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.sync.converter.ICalConverter
import com.thymeflow.sync.dav.{BaseDavSynchronizer, CalendarMultigetReport, CalendarQueryReport}
import com.thymeflow.update.UpdateResults
import org.openrdf.model.{Model, Resource, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
object CalDavSynchronizer extends BaseDavSynchronizer {

  def source(valueFactory: ValueFactory) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  case class Config(sardine: Sardine, baseUri: String) {
  }

  private class Publisher(valueFactory: ValueFactory)
    extends BaseDavPublisher[DocumentsFetcher](valueFactory) {

    override def receive: Receive = super.receive orElse {
      case config: Config =>
        addFetcher(new DocumentsFetcher(valueFactory, config.sardine, config.baseUri))
    }
  }

  private class DocumentsFetcher(valueFactory: ValueFactory, sardine: Sardine, baseUri: String)
    extends BaseDavDocumentsFetcher(valueFactory, sardine, baseUri) {

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

    override protected def convert(str: String, context: Resource): Model = {
      iCalConverter.convert(str, context)
    }

    override def applyDiff(str: String, diff: ModelDiff): (String, UpdateResults) = {
      iCalConverter.applyDiff(str, diff)
    }
  }
}
