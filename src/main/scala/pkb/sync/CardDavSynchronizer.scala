package pkb.sync

import javax.xml.namespace.QName

import akka.actor.Props
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import org.openrdf.model.{Model, ValueFactory}
import pkb.rdf.model.document.Document
import pkb.sync.converter.VCardConverter
import pkb.sync.dav._

/**
  * @author Thomas Pellissier Tanon
  */
object CardDavSynchronizer extends BaseDavSynchronizer {

  def source(valueFactory: ValueFactory) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  case class Config(sardine: Sardine, baseUri: String) {
  }

  private class Publisher(valueFactory: ValueFactory)
    extends BaseDavPublisher[DocumentsFetcher](valueFactory) {

    override def receive: Receive = {
      case Request(_) =>
        deliverDocuments()
      case config: Config =>
        addFetcher(new DocumentsFetcher(valueFactory, config.sardine, config.baseUri))
        deliverDocuments()
      case Cancel =>
        context.stop(self)
    }
  }

  private class DocumentsFetcher(valueFactory: ValueFactory, sardine: Sardine, baseUri: String)
    extends BaseDavDocumentsFetcher(valueFactory, sardine, baseUri) {

    private val CardDavNamespace = "urn:ietf:params:xml:ns:carddav"
    private val vCardConverter = new VCardConverter(valueFactory)

    override protected def dataNodeName = new QName(CardDavNamespace, "address-data")

    override protected def buildQueryReport(withData: Boolean): SardineReport[Traversable[DavResource]] = {
      new AddressbookQueryReport(withData)
    }

    override protected def buildMultigetReport(paths: Traversable[String]): SardineReport[Traversable[DavResource]] = {
      new AddressbookMultigetReport(paths)
    }

    override protected def convert(str: String): Model = {
      vCardConverter.convert(str)
    }
  }
}
