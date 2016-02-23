package pkb.sync

import javax.xml.namespace.QName

import com.github.sardine.Sardine
import com.github.sardine.impl.SardineException
import org.apache.http.client.utils.URIBuilder
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.{Model, ValueFactory}
import pkb.sync.converter.ICalConverter
import pkb.sync.dav.CalendarQueryReport

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
class CalDavSynchronizer(valueFactory: ValueFactory, sardine: Sardine) {

  private val CalDavNamespace = "urn:ietf:params:xml:ns:caldav"
  private val CalendarData = new QName(CalDavNamespace, "calendar-data")

  private val iCalConverter = new ICalConverter(valueFactory)

  def synchronize(base: String): Model = {
    val model = new LinkedHashModel
    for (addressBookUri <- getCalendarUris(base)) {
      model.addAll(getCalendar(addressBookUri))
    }
    model
  }

  private def getCalendarUris(base: String): Iterable[String] = {
    sardine.list(base.toString, 0).asScala.map(resource => new URIBuilder(base).setPath(resource.getPath).toString)
  }

  def getCalendar(calendarUri: String): Model = {
    val model = new LinkedHashModel
    try {
      for (resource <- sardine.report(calendarUri, 1, new CalendarQueryReport)) {
        //TODO: use the entry URI as URI for the ICal Event?
        Option(resource.getCustomPropsNS.get(CalendarData)).foreach(ical => model.addAll(iCalConverter.convert(ical)))
      }
    } catch {
      case e: SardineException => e.printStackTrace()
    }
    model
  }
}
