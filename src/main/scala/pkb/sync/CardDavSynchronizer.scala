package pkb.sync

import javax.xml.namespace.QName

import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import org.openrdf.model.{Model, ValueFactory}
import pkb.sync.converter.VCardConverter
import pkb.sync.dav.{AddressbookQueryReport, BaseDavSynchronizer}

/**
  * @author Thomas Pellissier Tanon
  */
class CardDavSynchronizer(valueFactory: ValueFactory, sardine: Sardine, baseUri: String)
  extends BaseDavSynchronizer(valueFactory, sardine, baseUri) {

  private val CardDavNamespace = "urn:ietf:params:xml:ns:carddav"
  private val vCardConverter = new VCardConverter(valueFactory)

  override protected def dataNodeName = new QName(CardDavNamespace, "address-data")

  override protected def buildReport: SardineReport[Traversable[DavResource]] = {
    new AddressbookQueryReport
  }

  override protected def convert(str: String): Model = {
    vCardConverter.convert(str)
  }
}
