package com.thymeflow.spatial.geocoding

import java.nio.file.{Files, Path}
import java.util.concurrent.Executors

import com.thymeflow.spatial.SimpleAddress
import com.thymeflow.spatial.geocoding.CachedGeocoder.CachedGeocoderProtocol
import com.thymeflow.spatial.geographic.Point
import com.thymeflow.spatial.geographic.impl.{Point => SimplePoint}
import com.typesafe.scalalogging.StrictLogging
import org.mapdb.{DB, DBMaker, StoreDirect, StoreWAL}
import spray.json._

import scala.collection.mutable
import scala.concurrent.{Future, _}


/**
  * @author Thomas pellissier Tanon
  * @author David Montoya
  */
object CachedGeocoder {

  object CachedGeocoderProtocol extends DefaultJsonProtocol {
    implicit val pointFormat = jsonFormat(SimplePoint, "longitude", "latitude")
    implicit val simpleAddressFormat = jsonFormat6(SimpleAddress)
    implicit val simpleFeatureSourceFormat = jsonFormat2(SimpleFeatureSource)
    implicit val simpleFeatureFormat = jsonFormat(
      (name: Option[String], point: SimplePoint, address: SimpleAddress, source: SimpleFeatureSource) => SimpleFeature(name, point, address, source), "name", "point", "address", "source")
  }

}

class CachedGeocoder(geocoder: Geocoder, persistentCachePath: Option[Path] = None) extends Geocoder with StrictLogging {
  // since MapDB code is blocking, we need to create an independent ExecutionContext for wrapping MapDB code within Futures.
  // otherwise, we risk running into a deadlock
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1000))

  private val mapDbMaker = persistentCachePath
    .map(file => {
      Files.createDirectories(file.getParent)
      logger.info(s"Geocoder cache is stored in file $file")
      DBMaker.newFileDB(file.toFile)
    })
    .getOrElse({
      logger.info(s"Geocoder cache is stored in memory")
      DBMaker.newMemoryDirectDB
    })
    .compressionEnable()
    .closeOnJvmShutdown()

  private val mapDb: DB = {
    try {
      mapDbMaker.make()
    } catch {
      case e: RuntimeException if e.getCause.isInstanceOf[ClassNotFoundException] =>
        // the previous cache was using POJO serializers
        // delete the cache before trying to create the Database
        persistentCachePath match {
          case Some(path) =>
            // Delete MapDB database files
            Files.deleteIfExists(path.resolveSibling(path.getFileName + StoreDirect.DATA_FILE_EXT))
            Files.deleteIfExists(path.resolveSibling(path.getFileName + StoreWAL.TRANS_LOG_FILE_EXT))
            Files.deleteIfExists(path)
            mapDbMaker.make()
          case None => throw e
        }
    }
  }

  import CachedGeocoderProtocol._

  private def getOrMakeHashMap[K, V](name: String)(implicit keyJsonFormat: JsonFormat[K], jsonFormat: JsonFormat[V]): mutable.Map[K, V] = {
    val baseMap = mapDb.getHashMap[String, String](name)
    new mutable.Map[K, V] {
      override def +=(kv: (K, V)): this.type = {
        baseMap.put(kv._1.toJson.toString, kv._2.toJson.toString)
        mapDb.commit()
        this
      }

      override def -=(key: K): this.type = {
        baseMap.remove(key.toJson.toString)
        mapDb.commit()
        this
      }

      override def get(key: K): Option[V] = {
        Option(baseMap.get(key.toJson.toString)).map(JsonParser(_).convertTo[V])
      }

      override def iterator: Iterator[(K, V)] = {
        import scala.collection.JavaConverters._
        baseMap.entrySet().iterator().asScala.map {
          entry => (JsonParser(entry.getKey).convertTo[K], JsonParser(entry.getValue).convertTo[V])
        }
      }
    }
  }

  private val reverseCache = getOrMakeHashMap[SimplePoint, Vector[SimpleFeature]]("geocoder-reverse-json")
  private val directCache = getOrMakeHashMap[String, Vector[SimpleFeature]]("geocoder-direct-json")
  private val directWithBiasCache = getOrMakeHashMap[(String, SimplePoint), Vector[SimpleFeature]]("geocoder-direct-bias-json")

  override def reverse(point: Point): Future[Traversable[Feature]] = getOrSet(reverseCache, (point: Point) => geocoder.reverse(point).map(featuresToSimpleFeatures))(SimplePoint(point.longitude, point.latitude))

  private def getOrSet[K](cache: mutable.Map[K, Vector[SimpleFeature]], set: K => Future[Traversable[Feature]])(key: K): Future[Traversable[Feature]] = {
    Future {
      cache.get(key)
    }.flatMap {
      case Some(value) => Future.successful(value)
      case None =>
        set(key).map(value => {
          cache += key -> featuresToSimpleFeatures(value)
          value
        })
    }
  }

  override def direct(address: String): Future[Traversable[Feature]] = getOrSet(directCache, (address: String) => geocoder.direct(address).map(featuresToSimpleFeatures))(address)

  override def direct(address: String, locationBias: Point): Future[Traversable[Feature]] =
    getOrSet(directWithBiasCache, parentDirectWithTuple)((address, SimplePoint(locationBias.longitude, locationBias.latitude)))

  def featuresToSimpleFeatures(features: Traversable[Feature]) = {
    features.map {
      feature => SimpleFeature(name = feature.name,
        point = SimplePoint(feature.point.longitude, feature.point.latitude),
        address = SimpleAddress(feature.address.houseNumber,
          feature.address.street,
          feature.address.locality,
          feature.address.postalCode,
          feature.address.region, feature.address.country),
        source = SimpleFeatureSource(isValid = feature.source.isValid,
          iri = feature.source.iri)
      )
    }.toVector
  }

  private def parentDirectWithTuple(tuple: (String, Point)): Future[Traversable[Feature]] = geocoder.direct(tuple._1, tuple._2).map(featuresToSimpleFeatures)

  /**
    * Closes the cache's Database.
    * All other methods will throw 'IllegalAccessError' after this method was called.
    */
  def close() = {
    if (!mapDb.isClosed) {
      mapDb.close()
    }
  }
}
