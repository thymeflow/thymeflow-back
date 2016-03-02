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
import scala.collection.mutable

/**
  * @author Thomas Pellissier Tanon
  */
abstract class BaseDavSynchronizer(valueFactory: ValueFactory, sardine: Sardine, baseUri: String) extends Synchronizer {

  private var elementsEtag = new mutable.HashMap[String, String]()

  private var paths: Traversable[String] = null

  def synchronize(): Traversable[Document] = {
    if (paths == null) {
      paths = getDirectoryUris(baseUri)
    }
    paths.flatMap(directoryUri => getDirectory(directoryUri))
  }

  private def getDirectoryUris(base: String): Traversable[String] = {
    sardine.list(base.toString, 0).asScala.map(resource => buildUriFromBaseAndPath(base, resource.getPath))
  }

  private def getDirectory(directoryUri: String): Traversable[Document] = {
    documentsFromDavResources(davResourcesOfUpdatedDocuments(directoryUri: String), directoryUri)
  }

  private def davResourcesOfUpdatedDocuments(directoryUri: String): Traversable[DavResource] = {
    try {
      if (elementsEtag.isEmpty) {
        //We load everything
        sardine.report(directoryUri, 1, buildQueryReport(true))
      } else {
        //We load only new documents
        val newPaths = sardine.report(directoryUri, 1, buildQueryReport(false)).filter(davResource =>
          !elementsEtag.get(davResource.getPath).contains(davResource.getEtag)
        ).map(davResource => davResource.getPath)
        if (newPaths.isEmpty) {
          None
        } else {
          sardine.report(directoryUri, 1, buildMultigetReport(newPaths))
        }
      }
    } catch {
      case e: SardineException =>
        e.printStackTrace()
        None
    }
  }

  private def documentsFromDavResources(davResources: Traversable[DavResource], directoryUri: String): Traversable[Document] = {
    davResources.flatMap(resource => {
      elementsEtag.put(resource.getPath, resource.getEtag)
      Option(resource.getCustomPropsNS.get(dataNodeName)).map(data =>
        new Document(valueFactory.createIRI(buildUriFromBaseAndPath(directoryUri, resource.getPath)), convert(data))
      )
    })
  }

  private def buildUriFromBaseAndPath(base: String, path: String): String = {
    new URIBuilder(base).setPath(path).toString
  }

  protected def dataNodeName: QName

  protected def buildQueryReport(withData: Boolean): SardineReport[Traversable[DavResource]]

  protected def buildMultigetReport(paths: Traversable[String]): SardineReport[Traversable[DavResource]]

  protected def convert(str: String): Model
}
