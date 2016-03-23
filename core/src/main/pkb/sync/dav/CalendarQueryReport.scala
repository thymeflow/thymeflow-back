package pkb.sync.dav

import com.github.sardine.DavResource
import com.github.sardine.model.Multistatus
import com.github.sardine.report.SardineReport

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class CalendarQueryReport(withData: Boolean) extends SardineReport[Traversable[DavResource]] {
  def toJaxb: AnyRef = {
    null
  }

  override def toXml: String = {
    var xml = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>" +
      "<cal:calendar-query xmlns:d=\"DAV:\" xmlns:cal=\"urn:ietf:params:xml:ns:caldav\">" +
      "<d:prop>" +
      "<d:getetag />"
    if(withData) {
      xml += "<cal:calendar-data />"
    }
    xml +
      "</d:prop>" +
      "<cal:filter>" +
      "<cal:comp-filter name=\"VCALENDAR\" />" +
      "</cal:filter>" +
      "</cal:calendar-query>"
  }

  def fromMultistatus(multistatus: Multistatus): Traversable[DavResource] = {
    multistatus.getResponse.asScala.map(response => new DavResource(response))
  }
}
