package com.thymeflow.text.search.elasticsearch

import java.io.{File, IOException, InputStream}
import java.util.UUID

import akka.actor.Scheduler
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.thymeflow.text.search.FullTextSearchAgent
import com.thymeflow.text.search.elasticsearch.ListenableActionFutureExtensions._
import com.thymeflow.text.search.elasticsearch.exceptions.{ElasticSearchAggregateException, ElasticSearchBulkException, ElasticSearchShardFailure}
import com.thymeflow.utilities.IO
import com.typesafe.scalalogging.StrictLogging
import org.elasticsearch.action.ShardOperationFailedException
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.settings.{ImmutableSettings, Settings}
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.node.NodeBuilder._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

/**
 * @author  David Montoya
 */
class FullTextSearchServer[T] private(indexName: String,
                                      entityDeserialize: String => T,
                                      entitySerialize: T => String,
                                      searchSize: Int = 100)(implicit executionContext: ExecutionContext, scheduler: Scheduler)
  extends StrictLogging with FullTextSearchAgent[T] {

  private val entityFieldName = "entity"
  private val valueFieldName = "value"
  private val esClient = FullTextSearchServer.esClient

  def refreshIndex() = {
    esClient.admin().indices().prepareRefresh(indexName).execute().future.map(x => handleShardFailures(x.getShardFailures))
  }

  def handleShardFailures(shardOperationFailures: Seq[ShardOperationFailedException]) = {
    val shardFailures = shardOperationFailures.map {
      failure =>
        new ElasticSearchShardFailure(Option(failure.index()), Option(failure.shardId()), Option(failure.reason()))
    }
    if (shardFailures.nonEmpty) {
      val exception = new ElasticSearchAggregateException("Shard failures:", shardFailures)
      throw exception
    }
  }

  override def close(): Future[Unit] = {
    deleteIndex()
  }

  def add(valuedEntities: Traversable[(T, String)])(implicit materializer: Materializer): Future[Unit] = {
    Source.fromIterator(() => valuedEntities.toIterator).grouped(500).mapAsync(1) {
      batchValuedEntities =>
        val bulkRequest = esClient.prepareBulk()
        batchValuedEntities.foreach {
          case (entity, value) =>
            bulkRequest.add(new IndexRequest(indexName).`type`("literal").source(literalJsonBuilder(entity, value)))
        }
        if (bulkRequest.numberOfActions() > 0) {
          bulkRequest.execute.future.map(handleBulkResponseFailures)
        } else {
          Future.successful(())
        }
    }.runForeach(_ => ()).map(_ => ())
  }

  private def handleBulkResponseFailures(response: BulkResponse): Unit = {
    val exceptions = response.getItems.filter(_.isFailed).map {
      item =>
        new ElasticSearchBulkException(Option(item.getIndex), Option(item.getType), Option(item.getId), Option(item.getFailureMessage))
    }
    if (exceptions.nonEmpty) {
      val exception = new ElasticSearchAggregateException("Bulk exceptions:", exceptions)
      throw exception
    }
  }

  private def literalJsonBuilder(entity: T, literal: String) = {
    XContentFactory.jsonBuilder().startObject().field(entityFieldName, entitySerialize(entity)).field(valueFieldName, literal).endObject()
  }

  override def matchQuery(query: String, matchPercent: Int = 100): Future[Seq[(T, String, Float)]] = {
    val queryBuilder = org.elasticsearch.index.query.QueryBuilders
      .matchQuery(valueFieldName, query)
      .minimumShouldMatch(matchPercent.toString + "%")
    val search = esClient.prepareSearch(indexName).setQuery(queryBuilder).setTypes("literal").setSize(searchSize)
    search.execute.future.map {
      case searchResponse =>
        handleShardFailures(searchResponse.getShardFailures)
        searchResponse.getHits.hits.toVector.map {
          case hit =>
            val (entity, value) = literalDeserializer(hit.sourceAsMap())
            (entity, value, hit.score())
        }
    }
  }

  private def literalDeserializer(source: java.util.Map[String, Object]) = {
    (entityDeserialize(source.get(entityFieldName).asInstanceOf[String]), source.get(valueFieldName).asInstanceOf[String])
  }

  private def recreateIndex() = {
    FullTextSearchServer.isClusterReady().flatMap {
      _ => deleteIndex().flatMap {
        case _ =>
          val mappings: InputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("textsearch/mappings.json")
          val indexSettings: InputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("textsearch/index_settings.json")
          esClient.admin.indices.prepareCreate(indexName).setSettings(IO.toString(indexSettings)).execute.future.flatMap {
            case _ =>
              esClient.admin.indices.preparePutMapping(indexName).setType("literal").setSource(IO.toString(mappings)).execute.future
          }
      }.map {
        case _ => ()
      }
    }
  }

  private def deleteIndex() = {
    esClient.admin.indices.prepareDelete(indexName).execute.future.map {
      case _ => ()
    }.recover {
      case e: IndexMissingException => ()
    }
  }

}

object FullTextSearchServer extends StrictLogging {

  final val dataDirectory = "data"
  final val clusterName = "thymeflow"
  lazy val (esNode, esClient) = {
    val sBuilder = ImmutableSettings.builder()
    sBuilder.put("path.home", this.esDirectory.toString)
    sBuilder.put("network.host", "127.0.0.1")
    sBuilder.put("threadpool.search.queue_size", "1000000")
    sBuilder.put("cluster.routing.allocation.disk.threshold_enabled", "false")
    val settings: Settings = sBuilder.build
    val esNode = nodeBuilder.clusterName(clusterName).loadConfigSettings(true).settings(settings).node
    logger.info("[elastic-search] Started search node.")
    (esNode, esNode.client)
  }

  import scala.concurrent.duration._

  def isClusterReady(checkPeriod: FiniteDuration = 5 seconds)
                    (implicit executionContext: ExecutionContext, scheduler: Scheduler): Future[Boolean] = {
    def execute() = {
      esClient.admin.cluster.preparePendingClusterTasks().execute().future.map {
        tasks => tasks.pendingTasks().isEmpty
      }.recover {
        case _ => false
      }
    }
    def recurrent(result: Future[Boolean]): Future[Boolean] = {
      result.flatMap {
        case false =>
          logger.info(s"Cluster is not ready, checking again in $checkPeriod.")
          akka.pattern.after(checkPeriod, scheduler)(recurrent(execute()))
        case true =>
          logger.info("Cluster is ready !")
          Future.successful(true)
      }
    }
    recurrent(execute())
  }
  private lazy val esDirectory: File = setupDirectories(dataDirectory)

  def apply[T](entityDeserialize: String => T)(entitySerialize: T => String, searchSize: Int = 100)(implicit executionContext: ExecutionContext, scheduler: Scheduler) = {
    // randomized index name
    val indexName = UUID.randomUUID().toString
    val server = new FullTextSearchServer[T](indexName, entityDeserialize, entitySerialize, searchSize)
    server.recreateIndex().map{
      case _ => server
    }
  }

  def shutdown(): Future[Unit] = {
    Future.successful({
      esClient.close()
      esNode.close()
      logger.info("[elastic-search] Stopped search node.")
      ()
    })
  }

  @throws(classOf[IOException])
  private def setupDirectories(directoryName: String) = {
    val esDirectory = new File(directoryName, "textsearch")
    val pluginDirectory: File = new File(esDirectory, "plugins")
    val scriptsDirectory: File = new File(esDirectory, "config/scripts")
    for (directory <- Array[File](esDirectory, pluginDirectory, scriptsDirectory)) {
      directory.mkdirs
    }
    esDirectory
  }
}
