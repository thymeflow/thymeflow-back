package thymeflow.text.search.elasticsearch.exceptions

/**
  * @author David Montoya
  */
class ElasticSearchException(arguments: Vector[(String, Option[Any])], cause: Option[Throwable] = None)
// cannot implement the following through a collect, due to a compile error
  extends Exception(arguments.filter(_._2.nonEmpty).map {
    case (name, value) => s"$name [${value.get}]"
  }.mkString(","), cause.orNull) {
}
