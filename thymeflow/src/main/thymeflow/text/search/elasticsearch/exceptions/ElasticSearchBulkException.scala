package thymeflow.text.search.elasticsearch.exceptions

/**
  * @author David Montoya
  */
class ElasticSearchBulkException(index: Option[String], `type`: Option[String], id: Option[String], message: Option[String])
  extends ElasticSearchException(Vector(("index", index), ("type", `type`), ("id", id), ("message", message))) {
}
