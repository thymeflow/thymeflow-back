package pkb.sync.dav

import javax.xml.namespace.QName

import com.github.sardine.impl.SardineException
import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import org.apache.http.client.utils.URIBuilder
import org.openrdf.model.{Model, ValueFactory}
import pkb.rdf.model.document.Document
import pkb.sync.Synchronizer

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
abstract class BaseDavSynchronizer(valueFactory: ValueFactory, sardine: Sardine, baseUri: String) extends Synchronizer {

  def synchronize(): Traversable[Document] = {
    getDirectoryUris(baseUri).flatMap(directoryUri => getDirectory(directoryUri))
  }

  private def getDirectoryUris(base: String): Traversable[String] = {
    sardine.list(base.toString, 0).asScala.map(resource => buildUriFromBaseAndPath(base, resource.getPath))
  }

  private def buildUriFromBaseAndPath(base: String, path: String): String = {
    new URIBuilder(base).setPath(path).toString
  }

  private def getDirectory(directoryUri: String): Traversable[Document] = {
    try {
      sardine.report(directoryUri, 1, buildReport).flatMap(resource =>
        //TODO: use the entry URI as URI for the ICal Event?
        Option(resource.getCustomPropsNS.get(dataNodeName)).map(data =>
          new Document(valueFactory.createIRI(buildUriFromBaseAndPath(directoryUri, resource.getPath)), convert(data))
        )
      )
    } catch {
      case e: SardineException =>
        e.printStackTrace()
        None
    }
  }

  protected def dataNodeName: QName

  protected def buildReport: SardineReport[Traversable[DavResource]]

  protected def convert(str: String): Model
}
