package pkb

import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.rio.{RDFFormat, Rio}
import pkb.sync.FileSynchronizer

object PKB {
  def main(args: Array[String]) {
    val syncronizer = new FileSynchronizer(SimpleValueFactory.getInstance)
    Rio.write(syncronizer.synchronize(args), System.out, RDFFormat.TURTLE)
  }
}
