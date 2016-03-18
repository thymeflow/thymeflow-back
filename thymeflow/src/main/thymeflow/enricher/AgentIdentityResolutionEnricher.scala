package thymeflow.enricher

import java.util.Locale

import com.typesafe.scalalogging.StrictLogging
import org.apache.lucene.search.spell.LevensteinDistance
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import pkb.rdf.Converters._
import pkb.rdf.model.vocabulary.SchemaOrg
import pkb.utilities.text.Normalization
import thymeflow.text.distances.BipartiteMatchingDistance
import thymeflow.text.search.elasticsearch.TextSearchServer

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}


/**
  * @author David Montoya
  */
class AgentIdentityResolutionEnricher(repositoryConnection: RepositoryConnection, val delay: Duration)
                                     (implicit val executionContext: ExecutionContext) extends DelayedEnricher with StrictLogging{

  // \u2022 is the bullet character
  val tokenSeparator =
    """[\p{Punct}\s\u2022]+""".r

  def entitySplit(content: String) = {
    tokenSeparator.split(content).toIndexedSeq
  }

  def normalizeTerm(term: String) = {
    Normalization.removeDiacriticalMarks(term).toLowerCase(Locale.ROOT)
  }

  def entityMatchingScore(combine: (Seq[String], Seq[String], Seq[(Seq[String], Seq[String], Double)]) => Double,
                          entityMatchingWeight: (Seq[String], Seq[String]) => Seq[(Seq[String],Seq[String], Double)])(text1: Seq[String], text2: Seq[String]) = {
    val weight = entityMatchingWeight(text1, text2)
    if (weight.nonEmpty) {
      val weightScore = combine(text1, text2, weight)
      Some((weight, weightScore))
    } else {
      None
    }
  }

  def entityMatching[T,X](matching: Traversable[(X, X, Seq[String], Seq[String], Seq[String])],
                          textMatch: (Seq[String], Seq[String]) => Option[(T, Double)]) = {
    matching.flatMap{
      case (s1, s2, text1, text2, matchText1) =>
        textMatch(text1, text2).map{
          case t => (s1, s2, matchText1, t)
        }
    }
  }

  def run() = {
    val metric = new LevensteinDistance()
    val entityMatchingWeight = new BipartiteMatchingDistance(
      (s1, s2) => 1.0d - metric.getDistance(normalizeTerm(s1), normalizeTerm(s2)), 0.3
    ).getDistance _
    val matchPercent = 80
    val tupleQuery =
      s"""
         |SELECT ?agent ?name ?emailAddress
         |WHERE {
         |  ?agent <${SchemaOrg.NAME}> ?name ;
         |         <${SchemaOrg.EMAIL}> ?x .
         |  ?x <${SchemaOrg.EMAIL}> ?emailAddress .
         | }
      """.stripMargin

    val result = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, tupleQuery).evaluate()
    val emailNameSet = new scala.collection.mutable.HashMap[String, (scala.collection.mutable.HashSet[String], scala.collection.mutable.HashMap[String, Int])]
    var i = 0
    result.foreach {
      bindingSet =>
        val (agent, name, emailAddress) = (bindingSet.getValue("agent").stringValue(), bindingSet.getValue("name").stringValue(), bindingSet.getValue("emailAddress").stringValue())
        val (agents, nameCounts) = emailNameSet.getOrElseUpdate(emailAddress, (new scala.collection.mutable.HashSet[String], new scala.collection.mutable.HashMap[String, Int]))
        i += 1
        if(i % 1000 == 0){
          logger.info(s"Iterated through $i rows.")
        }
        agents += agent
        nameCounts += name -> (nameCounts.getOrElse(name, 0) + 1)
    }
    logger.info(s"Iterated through $i rows.")
    val emailAndNames = emailNameSet.toTraversable.flatMap{
      case (email, (_, names)) =>
        names.keys.map{
          case name => (email, name)
        }
    }(scala.collection.breakOut):Vector[(String, String)]

    TextSearchServer {
      case (id) => emailAndNames(Integer.parseInt(id))
    }.flatMap{
      case textSearchServer =>
        val literalsToIndex = emailAndNames.zipWithIndex.map{
          case ((_, name), index) => (index.toString, name)
        }
        textSearchServer.add(literalsToIndex).flatMap{
          case _ =>
            textSearchServer.refreshIndex()
        }.map{
          case _ =>
            textSearchServer
        }
    }.map{
      case textSearchServer =>
        Future.sequence(emailAndNames.map{
          case (email1, name1) =>
            textSearchServer.search(name1, matchPercent).map {
              case matching =>
                val name1Split = entitySplit(name1)
                matching.collect {
                  case ((email2, name2), score) if email1 != email2 =>
                    (email1, name1Split, email2, entitySplit(name2))
                }
            }
        }).map(_.flatten)
    }
  }

}
