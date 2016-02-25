package pkb.sync

import javax.xml.namespace.QName

import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import org.openrdf.model.{Model, ValueFactory}
import pkb.sync.converter.ICalConverter
import pkb.sync.dav.{BaseDavSynchronizer, CalendarQueryReport}

/**
  * @author Thomas Pellissier Tanon
  */
class CalDavSynchronizer(valueFactory: ValueFactory, sardine: Sardine)
  extends BaseDavSynchronizer(valueFactory, sardine) {

  private val CalDavNamespace = "urn:ietf:params:xml:ns:caldav"
  private val iCalConverter = new ICalConverter(valueFactory)

  override protected def dataNodeName = new QName(CalDavNamespace, "calendar-data")

  override protected def buildReport: SardineReport[Iterable[DavResource]] = {
    new CalendarQueryReport
  }

  override protected def convert(str: String): Model = {
    iCalConverter.convert(str)
  }
}
