package thymeflow.enricher

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.repository.RepositoryConnection

import scala.concurrent.duration.Duration

/**
  * @author David Montoya
  */
class LocationStayStopEnricher(repositoryConnection: RepositoryConnection, val delay: Duration)
  extends DelayedEnricher with StrictLogging {

  override def runEnrichments(): Unit = {

  }
}
