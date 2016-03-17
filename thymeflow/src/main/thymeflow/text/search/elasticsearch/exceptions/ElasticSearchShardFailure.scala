package thymeflow.text.search.elasticsearch.exceptions

/**
  * @author David Montoya
  */
class ElasticSearchShardFailure(index: Option[String], shardId: Option[Int], reason: Option[String], cause: Option[Throwable])
  extends ElasticSearchException(Vector(("index", index), ("shardId", shardId), ("reason", reason), ("cause", cause)), cause) {
}
