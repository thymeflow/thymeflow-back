package com.thymeflow.text.search.elasticsearch.exceptions

/**
  * @author David Montoya
  */
class ElasticSearchAggregateException(message: String, exceptions: Seq[ElasticSearchException])
  extends Exception((message +: exceptions.map(_.getMessage)).mkString("\n")) {

}
