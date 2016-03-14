package pkb.sync

import javax.xml.namespace.QName

import akka.actor.Props
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.Source
import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import org.openrdf.model.{Model, ValueFactory}
import pkb.rdf.model.document.Document
import pkb.sync.converter.ICalConverter
import pkb.sync.dav.{BaseDavSynchronizer, CalendarMultigetReport, CalendarQueryReport}

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

    override def receive: Receive = {
      case Request =>
        deliverDocuments()
      case config: Config =>
        addFetcher(new DocumentsFetcher(valueFactory, config.sardine, config.baseUri))
        deliverDocuments()
    }
  }

  private class DocumentsFetcher(valueFactory: ValueFactory, sardine: Sardine, baseUri: String)
    extends BaseDavDocumentsFetcher(valueFactory, sardine, baseUri) {

    private val CalDavNamespace = "urn:ietf:params:xml:ns:caldav"
    private val iCalConverter = new ICalConverter(valueFactory)

    override protected def dataNodeName = new QName(CalDavNamespace, "calendar-data")

    override protected def buildQueryReport(withData: Boolean): SardineReport[Traversable[DavResource]] = {
      new CalendarQueryReport(withData)
    }

    override protected def buildMultigetReport(paths: Traversable[String]): SardineReport[Traversable[DavResource]] = {
      new CalendarMultigetReport(paths)
    }

    override protected def convert(str: String): Model = {
      iCalConverter.convert(str)
    }
  }
}
