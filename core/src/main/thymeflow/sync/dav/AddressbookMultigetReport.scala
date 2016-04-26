package thymeflow.sync.dav

import com.github.sardine.DavResource
import com.github.sardine.model.Multistatus
import com.github.sardine.report.SardineReport

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class AddressbookMultigetReport(paths: Traversable[String]) extends SardineReport[Traversable[DavResource]] {
  def toJaxb: AnyRef = {
    null
  }

  override def toXml: String = {
    var xml = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>" +
      "<card:addressbook-multiget xmlns:d=\"DAV:\" xmlns:card=\"urn:ietf:params:xml:ns:carddav\">" +
      "<d:prop>" +
      "<d:getetag />" +
      "<card:address-data />" +
      "</d:prop>"
    paths.foreach(path => xml += "<d:href>" + path + "</d:href>")
    xml +
      "</card:addressbook-multiget>"
  }

  def fromMultistatus(multistatus: Multistatus): Traversable[DavResource] = {
    multistatus.getResponse.asScala.map(response => new DavResource(response))
  }
}
