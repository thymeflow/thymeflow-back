package thymeflow.rdf

import java.io.{File, FileOutputStream}

import org.openrdf.model.Resource
import org.openrdf.repository.RepositoryConnection
import org.openrdf.rio.{Rio, UnsupportedRDFormatException}

import scala.compat.java8.OptionConverters._

/**
  * Allows to synchronize a repository with a file
  *
  * @author Thomas Pellissier Tanon
  */
case class FileSynchronization(repositoryConnection: RepositoryConnection, file: File, context: Resource) {

  def load(): Unit = {
    val parserFormat = Rio.getParserFormatForFileName(file.getName).asScala.getOrElse(
      throw new UnsupportedRDFormatException(s"Format not found for file $file")
    )
    repositoryConnection.add(file, null, parserFormat, context)
  }

  def save(): Unit = {
    val writerFormat = Rio.getWriterFormatForFileName(file.getName).asScala.getOrElse(
      throw new UnsupportedRDFormatException(s"Format not found for file $file")
    )
    val outputStream = new FileOutputStream(file)
    repositoryConnection.export(Rio.createWriter(writerFormat, outputStream), context)
    outputStream.close()
  }

  def saveOnJvmClose(): Unit = {
    sys.addShutdownHook {
      save()
    }
  }
}
