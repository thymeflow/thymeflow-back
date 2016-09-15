package com.thymeflow.utilities

import java.nio.file.{Files, Path}
import java.time.{Duration, Instant}
import java.util.concurrent.Executors

import com.thymeflow.utilities.Cached.{CachedMap, JsonProtocol, Value}
import com.typesafe.scalalogging.StrictLogging
import org.mapdb.{DB, DBMaker, StoreDirect, StoreWAL}
import spray.json.{DefaultJsonProtocol, JsonFormat, JsonParser, _}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

/**
  * Allows the creation of persistent disk caches.
  * Optionally, the cache can be kept in-memory (off-heap), however the cache wil lost on JVM shutdown.
  * Uses MapDB internally to maintain the cache.
  *
  * @author David Montoya
  */
trait Cached extends StrictLogging with JsonProtocol {

  protected def persistentCachePath: Option[Path]

  protected def cacheName: String

  private val mapDbMaker = persistentCachePath
    .map(file => {
      Files.createDirectories(file.getParent)
      logger.info(s"$cacheName cache is stored in file $file")
      DBMaker.newFileDB(file.toFile)
    })
    .getOrElse({
      logger.info(s"$cacheName cache is stored in memory")
      DBMaker.newMemoryDirectDB
    })
    .compressionEnable()
    .closeOnJvmShutdown()

  private val mapDb: DB = {
    try {
      mapDbMaker.make()
    } catch {
      case e: RuntimeException if e.getCause.isInstanceOf[ClassNotFoundException] =>
        // the previous implementation was using POJO serializers
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

  protected def getOrMakeHashMap[K, V](name: String,
                                       expiration: Duration = Duration.ofDays(365))
                                      (implicit keyJsonFormat: JsonFormat[K], jsonFormat: JsonFormat[V]): CachedMap[K, V] = {

    val baseMap = mapDb.getHashMap[String, String](name)
    val innerMap = new mutable.Map[K, V] {
      override def +=(kv: (K, V)): this.type = {
        val value = Value(modifiedAt = Instant.now(), content = kv._2)
        baseMap.put(kv._1.toJson.toString, value.toJson.toString)
        mapDb.commit()
        this
      }

      override def -=(key: K): this.type = {
        baseMap.remove(key.toJson.toString)
        mapDb.commit()
        this
      }

      override def get(key: K): Option[V] = {
        Option(baseMap.get(key.toJson.toString)).map(JsonParser(_).convertTo[Value[V]]).collect {
          case Value(modifiedAt, content) if Duration.between(modifiedAt, Instant.now).compareTo(expiration) <= 0 => content
        }
      }

      override def iterator: Iterator[(K, V)] = {
        import scala.collection.JavaConverters._
        baseMap.entrySet().iterator().asScala.map {
          entry => (JsonParser(entry.getKey).convertTo[K], JsonParser(entry.getValue).convertTo[V])
        }
      }
    }
    implicit val ec = Cached.ec
    new CachedMap[K, V] {
      def getOrSet(set: K => Future[V])(key: K): Future[V] = {
        Future {
          innerMap.get(key)
        }.flatMap {
          case Some(value) => Future.successful(value)
          case None =>
            set(key).map(value => {
              innerMap += key -> value
              value
            })
        }
      }

      def getOrSetSync(set: K => V)(key: K): V = {
        innerMap.getOrElseUpdate(key, set(key))
      }
    }
  }

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

object Cached {

  case class Value[V](modifiedAt: Instant, content: V)

  trait JsonProtocol extends DefaultJsonProtocol with JsonFormats.InstantJsonFormat {
    implicit def valueFormat[V](implicit contentFormat: JsonFormat[V]): RootJsonFormat[Value[V]] = jsonFormat2(Value[V])
  }

  // since MapDB code is blocking, we need to create an independent ExecutionContext for wrapping MapDB code within Futures.
  // otherwise, we risk running into a deadlock
  private val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1000))

  trait CachedMap[K, V] {
    def getOrSet(set: K => Future[V])(key: K): Future[V]

    def getOrSetSync(set: K => V)(key: K): V
  }

}