package thymeflow.spatial.geocoding

import java.io.File

import com.typesafe.scalalogging.StrictLogging
import org.mapdb.{DB, DBMaker}
import thymeflow.actors._
import thymeflow.spatial.geographic.Point

import scala.concurrent.Future

/**
  * @author Thomas pellissier Tanon
  */
class CachedGeocoder(geocoder: Geocoder, persistantCacheFile: Option[File] = None) extends Geocoder with StrictLogging {

  private val mapDb: DB = persistantCacheFile
    .map(file => {
      file.getParentFile.mkdirs()
      logger.info(s"Geocoder cache is stored in file $file")
      DBMaker.newFileDB(file)
    })
    .getOrElse({
      logger.info(s"Geocoder cache is stored in memory")
      DBMaker.newMemoryDirectDB
    })
    .compressionEnable()
    .closeOnJvmShutdown()
    .make()
  private val reverseCache: java.util.Map[Point, Traversable[Feature]] = mapDb.getHashMap("geocoder-reverse")
  private val directCache: java.util.Map[String, Traversable[Feature]] = mapDb.getHashMap("geocoder-direct")
  private val directWithBiasCache: java.util.Map[(String, Point), Traversable[Feature]] = mapDb.getHashMap("geocoder-direct-bias")

  override def reverse(point: Point): Future[Traversable[Feature]] = getOrSet(reverseCache, geocoder.reverse)(point)

  private def getOrSet[K, V](cache: java.util.Map[K, V], set: K => Future[V])(key: K): Future[V] = {
    if (cache.containsKey(key)) {
      Future {
        cache.get(key)
      }
    } else {
      val result = set(key)
      result.foreach(value => {
        cache.put(key, value)
        mapDb.commit()
      })
      result
    }
  }

  override def direct(address: String): Future[Traversable[Feature]] = getOrSet(directCache, geocoder.direct)(address)

  override def direct(address: String, locationBias: Point): Future[Traversable[Feature]] =
    getOrSet(directWithBiasCache, parentDirectWithTuple)((address, locationBias))

  private def parentDirectWithTuple(tuple: (String, Point)): Future[Traversable[Feature]] = geocoder.direct(tuple._1, tuple._2)
}
