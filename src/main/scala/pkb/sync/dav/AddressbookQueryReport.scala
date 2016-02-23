package pkb.sync.dav

import com.github.sardine.DavResource
import com.github.sardine.model.Multistatus
import com.github.sardine.report.SardineReport

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class AddressbookQueryReport extends SardineReport[Iterable[DavResource]] {
  def toJaxb: AnyRef = {
    null
  }

  override def toXml: String = {
    "<?xml version=\"1.0\" encoding=\"utf-8\" ?>" +
      "<card:addressbook-query xmlns:d=\"DAV:\" xmlns:card=\"urn:ietf:params:xml:ns:carddav\">" +
      "<d:prop>" +
      "<d:getetag />" +
      "<card:address-data />" +
      "</d:prop>" +
      "<card:filter>" +
      "<card:prop-filter name=\"FN\">" +
      "</card:prop-filter>" +
      "</card:filter>" +
      "</card:addressbook-query>"
  }

  def fromMultistatus(multistatus: Multistatus): Iterable[DavResource] = {
    multistatus.getResponse.asScala.map(response => new DavResource(response))
  }
}
