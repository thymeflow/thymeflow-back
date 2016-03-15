package thymeflow.textsearch.elasticsearch

import java.io.{File, IOException, InputStream}
import java.util.UUID

import com.typesafe.scalalogging.StrictLogging
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.node.NodeBuilder._
import org.json4s.jackson.Serialization._
import thymeflow.textsearch.TextSearchAgent
import thymeflow.utilities.IO
import thymeflow.textsearch.elasticsearch.ListenableActionFutureExtensions._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
 * @author  David Montoya
 */
case class Literal(value: String)

class Server[T] private (indexName: String, idToEntity: String => T, searchSize: Int = 100)(implicit executionContext: ExecutionContext)
  extends StrictLogging with TextSearchAgent[T]{

  private val esClient = Server.esClient

  private def recreateIndex() = {
    deleteIndex().flatMap{
      case _ =>
        val mappings: InputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("elasticsearch/mappings.json")
        val indexSettings: InputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("elasticsearch/index_settings.json")
        esClient.admin.indices.prepareCreate(indexName).setSettings(IO.toString(indexSettings)).execute.future.flatMap{
          case _ => esClient.admin.indices.preparePutMapping(indexName).setType("literal").setSource(IO.toString(mappings)).execute.future
        }
    }.map{
      case _ => ()
    }
  }

  private def deleteIndex() = {
    esClient.admin.indices.prepareDelete(indexName).execute.future.map{
      case _ => ()
    }.recover{
      case e: IndexNotFoundException => ()
    }
  }


  override def close(): Future[Unit] = {
    deleteIndex()
  }

  private def add(entities: Traversable[(String,String)]): Future[Unit] = {
    implicit val formats = org.json4s.DefaultFormats
    val bulkRequest = esClient.prepareBulk()
    entities.foreach {
      case (id, literal) =>
        bulkRequest.add(new IndexRequest(indexName).`type`("literal").id(id).source(write(Literal(literal))))
    }
    bulkRequest.execute.future.flatMap{
      case _ =>
        esClient.admin().indices().prepareRefresh(indexName).execute().future.map(_ => ())
    }
  }

  def analyze(literal: String) = {
    val builder = XContentFactory.jsonBuilder().startObject().field("value", literal).endObject()
    val query = esClient.prepareTermVector().setType("literal").setIndex(indexName).setDoc(builder)
    query.execute().future.map{
      case response =>
        response.getFields.iterator().asScala.mkString("\n")
    }
  }

  override def search(query: String, matchPercent: Int = 100): Future[Seq[(T,Float)]] = {
    val queryBuilder = org.elasticsearch.index.query.QueryBuilders
      .matchQuery("value", query)
      //.analyzer("index_ngram")
      .minimumShouldMatch(matchPercent.toString + "%")
    val search = esClient.prepareSearch(indexName).setQuery(queryBuilder).setTypes("literal").setSize(searchSize)
    search.execute.future.map{
      case searchResponse =>
        searchResponse.getHits.hits.toVector.map{
          case hit =>
            (idToEntity(hit.id()), hit.score())
        }
    }
  }

}

object Server extends StrictLogging{

  final val dataDirectory = "data"
  final val clusterName = "trahup"

  def apply[T](entities: Traversable[(T, String)])(implicit executionContext: ExecutionContext) = {
    val indexedEntities = entities.toIndexedSeq
    // randomized index name
    val indexName = UUID.randomUUID().toString
    val idToEntity = (id: String) => indexedEntities(Integer.parseInt(id))._1
    val server = new Server[T](indexName, idToEntity)
    server.recreateIndex().flatMap{
      _ =>
        server.add(indexedEntities.zipWithIndex.map{ case (x, index) => (index.toString, x._2)}).map{
          _ => server
        }

    }
  }

  @throws(classOf[IOException])
  private def setupDirectories(directoryName: String) = {
    val esDirectory = new File(directoryName, "elasticsearch")
    val pluginDirectory: File = new File(esDirectory, "plugins")
    val scriptsDirectory: File = new File(esDirectory, "config/scripts")
    for (directory <- Array[File](esDirectory, pluginDirectory, scriptsDirectory)) {
      directory.mkdirs
    }
    esDirectory
  }

  private lazy val esDirectory: File = setupDirectories(dataDirectory)

  lazy val (esNode, esClient) = {
    val sBuilder = Settings.builder()
    sBuilder.put("path.home", this.esDirectory.toString)
    sBuilder.put("network.host", "127.0.0.1")
    sBuilder.put("threadpool.search.queue_size", "1000000")
    val settings: Settings = sBuilder.build
    val esNode = nodeBuilder.clusterName(clusterName).settings(settings).node
    logger.info("[elastic-search] Started search node.")
    (esNode, esNode.client())
  }

  def shutdown(): Future[Unit] = {
    Future.successful({
      esClient.close()
      esNode.close()
    })
  }
}
