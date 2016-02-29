package pkb.sync.dav

import javax.xml.namespace.QName

import com.github.sardine.impl.SardineException
import com.github.sardine.report.SardineReport
import com.github.sardine.{DavResource, Sardine}
import org.apache.http.client.utils.URIBuilder
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.{IRI, Model, ValueFactory}

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  */
abstract class BaseDavSynchronizer(valueFactory: ValueFactory, sardine: Sardine) {

  def synchronize(base: String): Model = {
    val model = new LinkedHashModel
    for (directoryUri <- getDirectoryUris(base)) {
      model.addAll(getDirectory(directoryUri))
    }
    model
  }

  private def getDirectoryUris(base: String): Traversable[String] = {
    sardine.list(base.toString, 0).asScala.map(resource => buildUriFromBaseAndPath(base, resource.getPath))
  }

  private def buildUriFromBaseAndPath(base: String, path: String): String = {
    new URIBuilder(base).setPath(path).toString
  }

  private def getDirectory(directoryUri: String): Model = {
    val model = new LinkedHashModel
    try {
      sardine.report(directoryUri, 1, buildReport).foreach(resource =>
        //TODO: use the entry URI as URI for the ICal Event?
        Option(resource.getCustomPropsNS.get(dataNodeName)).foreach(data =>
          addWithContext(convert(data), model, valueFactory.createIRI(buildUriFromBaseAndPath(directoryUri, resource.getPath)))
        )
      )
    } catch {
      case e: SardineException => e.printStackTrace()
    }
    model
  }

  private def addWithContext(fromModel: Model, toModel: Model, context: IRI): Unit = {
    for (statement <- fromModel.iterator().asScala) {
      toModel.add(statement.getSubject, statement.getPredicate, statement.getObject, context)
    }
  }

  protected def dataNodeName: QName

  protected def buildReport: SardineReport[Traversable[DavResource]]

  protected def convert(str: String): Model
}
