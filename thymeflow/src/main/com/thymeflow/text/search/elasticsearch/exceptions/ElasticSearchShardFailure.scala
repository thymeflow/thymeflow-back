package com.thymeflow.text.search.elasticsearch.exceptions

/**
  * @author David Montoya
  */
class ElasticSearchShardFailure(index: Option[String], shardId: Option[Int], reason: Option[String])
  extends ElasticSearchException(Vector(("index", index), ("shardId", shardId), ("reason", reason))) {
}
