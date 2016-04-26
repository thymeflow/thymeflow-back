package thymeflow.sync.dav

import com.github.sardine.DavResource
import com.github.sardine.model.Multistatus
import com.github.sardine.report.SardineReport

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class AddressbookQueryReport(withData: Boolean) extends SardineReport[Traversable[DavResource]] {
  def toJaxb: AnyRef = {
    null
  }

  override def toXml: String = {
    var xml = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>" +
      "<card:addressbook-query xmlns:d=\"DAV:\" xmlns:card=\"urn:ietf:params:xml:ns:carddav\">" +
      "<d:prop>" +
      "<d:getetag />"
    if(withData) {
      xml += "<card:address-data />"
    }
    xml +
      "</d:prop>" +
      "<card:filter>" +
      "<card:prop-filter name=\"FN\">" +
      "</card:prop-filter>" +
      "</card:filter>" +
      "</card:addressbook-query>"
  }

  def fromMultistatus(multistatus: Multistatus): Traversable[DavResource] = {
    multistatus.getResponse.asScala.map(response => new DavResource(response))
  }
}
