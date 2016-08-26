package com.thymeflow.sync

import javax.xml.namespace.QName

import akka.actor.Props
import akka.stream.scaladsl.Source
import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import com.thymeflow.rdf.model.ModelDiff
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.sync.converter.VCardConverter
import com.thymeflow.sync.dav._
import com.thymeflow.update.UpdateResults
import com.typesafe.config.{Config => AppConfig}
import org.openrdf.model.{Model, Resource, ValueFactory}

/**
  * @author Thomas Pellissier Tanon
  */
object CardDavSynchronizer extends BaseDavSynchronizer {

  def source(valueFactory: ValueFactory)(implicit config: AppConfig) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  case class Config(sardine: Sardine, baseUri: String)

  private class Publisher(valueFactory: ValueFactory)(implicit appConfig: AppConfig)
    extends BaseDavPublisher[DocumentsFetcher](valueFactory) {

    override def receive: Receive = super.receive orElse {
      case config: Config =>
        addFetcher(new DocumentsFetcher(valueFactory, config.sardine, config.baseUri))
    }
  }

  private class DocumentsFetcher(valueFactory: ValueFactory, sardine: Sardine, baseUri: String)(implicit appConfig: AppConfig)
    extends BaseDavDocumentsFetcher(valueFactory, sardine, baseUri) {

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

    override protected def convert(str: String, context: Resource): Model = {
      vCardConverter.convert(str, context)
    }

    override def applyDiff(str: String, diff: ModelDiff): (String, UpdateResults) = {
      vCardConverter.applyDiff(str, diff)
    }
  }
}
