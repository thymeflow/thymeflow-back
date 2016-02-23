package pkb.sync

import javax.xml.namespace.QName

import com.github.sardine.Sardine
import org.apache.http.client.utils.URIBuilder
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.{Model, ValueFactory}
import pkb.sync.converter.VCardConverter
import pkb.sync.converter.dav.AddressbookQueryReport

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class CardDavSynchronizer(valueFactory: ValueFactory, sardine: Sardine) {

  private val CardDavNamespace = "urn:ietf:params:xml:ns:carddav"
  private val AddressData = new QName(CardDavNamespace, "address-data")

  private val vCardConverter = new VCardConverter(valueFactory)

  def synchronize(base: String): Model = {
    val model = new LinkedHashModel
    for (addressBookUri <- getAddressBooksUris(base)) {
      model.addAll(getAddressBook(addressBookUri))
    }
    model
  }

  private def getAddressBooksUris(base: String): Iterable[String] = {
    sardine.list(base.toString).asScala.map(resource => new URIBuilder(base).setPath(resource.getPath).toString)
  }

  private def getAddressBook(addressBookUri: String): Model = {
    val model = new LinkedHashModel
    for (resource <- sardine.report(addressBookUri, -1, new AddressbookQueryReport)) {
      //TODO: use the VCard URI as URI for the VCard Person?
      model.addAll(vCardConverter.convert(resource.getCustomPropsNS.get(AddressData)))
    }
    model
  }
}
