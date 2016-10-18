package com.thymeflow.text.search.elasticsearch

import akka.stream.scaladsl.Source
import com.thymeflow.actors._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.Span
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author David Montoya
  */
class FullTextSearchServerSpec extends FlatSpec with Matchers with ScalaFutures {
  "FullTextSearch" should "find simple texts" in {
    val texts = Vector("John Doe", "Does John", "Alice Wonders", "Alic Wondrs")
    val queries = Vector("John", "Doe", "Alice", "Wonders")
    val results = Vector(Set("John Doe", "Does John"), Set("John Doe", "Does John"), Set("Alice Wonders", "Alic Wondrs"), Set("Alice Wonders"))

    val matchPercent = 80
    implicit val scheduler = system.scheduler
    val resultFuture = FullTextSearchServer(identity)(identity).flatMap {
      case server =>
        server.add(texts.zip(texts)).flatMap {
          _ =>
            server.refreshIndex().flatMap{
              _ =>
                Source(queries).mapAsync(1) {
                  case query =>
                    server.matchQuery(query, matchPercent).map {
                      _.map(_._2).toSet
                    }
                }.runFold(Vector.empty[Set[String]])(_ :+ _)
            }
        }
    }
    resultFuture.map {
      case future => future
    }
    resultFuture.futureValue(Timeout(Span.Max)) should equal(results)
  }
}
