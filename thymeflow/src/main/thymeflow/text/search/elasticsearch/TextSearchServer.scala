package thymeflow.text.search.elasticsearch

import java.io.{File, IOException, InputStream}
import java.util.UUID

import com.typesafe.scalalogging.StrictLogging
import org.elasticsearch.action.ShardOperationFailedException
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.settings.{ImmutableSettings, Settings}
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.node.NodeBuilder._
import thymeflow.text.search.TextSearchAgent
import thymeflow.text.search.elasticsearch.ListenableActionFutureExtensions._
import thymeflow.text.search.elasticsearch.exceptions.{ElasticSearchAggregateException, ElasticSearchBulkException, ElasticSearchShardFailure}
import thymeflow.utilities.IO

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
 * @author  David Montoya
 */
class TextSearchServer[T] private(indexName: String, entityMap: (String, String) => T, searchSize: Int = 100)(implicit executionContext: ExecutionContext)
  extends StrictLogging with TextSearchAgent[T]{

  private val valueFieldName = "value"
  private val esClient = TextSearchServer.esClient

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

  private def deleteIndex() = {
    esClient.admin.indices.prepareDelete(indexName).execute.future.map {
      case _ => ()
    }.recover {
      case e: IndexMissingException => ()
    }
  }

  def add(entities: Traversable[(String, String)]): Future[Unit] = {
    implicit val formats = org.json4s.DefaultFormats
    val bulkRequest = esClient.prepareBulk()
    entities.foreach {
      case (id, literal) =>
        bulkRequest.add(new IndexRequest(indexName).`type`("literal").id(id).source(literalJsonBuilder(literal)))
    }
    bulkRequest.execute.future.map(handleBulkResponseFailures)
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

  private def literalJsonBuilder(literal: String) = {
    XContentFactory.jsonBuilder().startObject().field(valueFieldName, literal).endObject()
  }

  def analyze(literal: String) = {
    val query = esClient.prepareTermVector().setType("literal").setIndex(indexName).setDoc(literalJsonBuilder(literal))
    query.execute().future.map {
      case response =>
        response.getFields.iterator().asScala.mkString("\n")
    }
  }

  override def search(query: String, matchPercent: Int = 100): Future[Seq[(T, Float)]] = {
    val queryBuilder = org.elasticsearch.index.query.QueryBuilders
      .matchQuery(valueFieldName, query)
      .minimumShouldMatch(matchPercent.toString + "%")
    val search = esClient.prepareSearch(indexName).setQuery(queryBuilder).setTypes("literal").setSize(searchSize)
    search.execute.future.map {
      case searchResponse =>
        handleShardFailures(searchResponse.getShardFailures)
        searchResponse.getHits.hits.toVector.map {
          case hit =>
            (entityMap(hit.id(), hit.sourceAsMap().get(valueFieldName).asInstanceOf[String]), hit.score())
        }
    }
  }

  private def recreateIndex() = {
    deleteIndex().flatMap {
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

object TextSearchServer extends StrictLogging {

  final val dataDirectory = "data"
  final val clusterName = "thymeflow"
  lazy val (esNode, esClient) = {
    val sBuilder = ImmutableSettings.builder()
    sBuilder.put("path.home", this.esDirectory.toString)
    sBuilder.put("network.host", "127.0.0.1")
    sBuilder.put("threadpool.search.queue_size", "1000000")
    val settings: Settings = sBuilder.build
    val esNode = nodeBuilder.clusterName(clusterName).loadConfigSettings(true).settings(settings).node
    logger.info("[elastic-search] Started search node.")
    (esNode, esNode.client())
  }
  private lazy val esDirectory: File = setupDirectories(dataDirectory)

  def apply[T](entityMap: (String, String) => T)(implicit executionContext: ExecutionContext) = {
    // randomized index name
    val indexName = UUID.randomUUID().toString
    val server = new TextSearchServer[T](indexName, entityMap)
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
