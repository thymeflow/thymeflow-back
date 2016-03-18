package thymeflow.enricher

import java.util.Locale

import com.typesafe.scalalogging.StrictLogging
import org.apache.lucene.search.spell.LevensteinDistance
import org.openrdf.model.vocabulary.RDF
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import pkb.rdf.Converters._
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.utilities.text.Normalization
import thymeflow.graph.ConnectedComponents
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

  def run() = {
    val metric = new LevensteinDistance()
    val distance = new BipartiteMatchingDistance(
      (s1, s2) => 1.0d - metric.getDistance(normalizeTerm(s1), normalizeTerm(s2)), 0.3
    )
    val entityMatchingWeight = distance.getDistance _
    val entityMatchIndices = distance.matchIndices _
    val matchPercent = 80

    val agentEmailAddressesQuery =
      s"""
         |SELECT ?agent ?emailAddress
         |WHERE {
         |  ?agent <${SchemaOrg.EMAIL}> ?x .
         |  ?x <${SchemaOrg.NAME}> ?emailAddress .
         | }
      """.stripMargin

    val agentFacetEmailAddresses = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, agentEmailAddressesQuery).evaluate().map {
      bindingSet =>
        (bindingSet.getValue("agent").stringValue(), bindingSet.getValue("emailAddress").stringValue())
    }

    val agentFacetEmailAddressesByEmailAddress = agentFacetEmailAddresses.groupBy(_._2).mapValues(_.map(_._1))
    val agentFacetEmailAddressesByAgentFacet = agentFacetEmailAddresses.groupBy(_._1).mapValues(_.map(_._2))

    val emailAgentFacetComponents = ConnectedComponents.compute[Either[String, String]](agentFacetEmailAddressesByEmailAddress.keys.map(Right(_)), {
      case Left(agent) => agentFacetEmailAddressesByAgentFacet.getOrElse(agent, Vector.empty).map(Right.apply)
      case Right(emailAddress) => agentFacetEmailAddressesByEmailAddress.getOrElse(emailAddress, Vector.empty).map(Left.apply)
    }).map {
      case component =>
        val agentFacets = component.collect {
          case Left(agent) => agent
        }
        val emailAddresses = component.collect {
          case Right(emailAddress) => emailAddress
        }
        (agentFacets.head, agentFacets, emailAddresses)
    }

    val agentFacetRepresentativeMap = emailAgentFacetComponents.flatMap {
      case (representative, agentFacets, _) =>
        agentFacets.map {
          case (agentFacet) =>
            (agentFacet, representative)
        }
    }.toMap

    agentFacetEmailAddresses.groupBy(_._2)

    val agentNamesQuery =
      s"""
         |SELECT ?agent ?name
         |WHERE {
         |  ?agent <${SchemaOrg.NAME}> ?name .
         |  ?agent <${RDF.TYPE}> <${Personal.AGENT}>
         | }
      """.stripMargin

    val agentFacetRepresentativeToNames = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[String, Int]]

    repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, agentNamesQuery).evaluate().foreach {
      bindingSet =>
        val (agent, name) = (bindingSet.getValue("agent").stringValue(), bindingSet.getValue("name").stringValue())
        val agentRepresentative = agentFacetRepresentativeMap.getOrElse(agent, agent)
        val nameCounts = agentFacetRepresentativeToNames.getOrElseUpdate(agentRepresentative, new scala.collection.mutable.HashMap[String, Int])
        nameCounts += name -> (nameCounts.getOrElse(name, 0) + 1)
    }

    val reconciliatedAgentNames = reconciliateAgentNames(agentFacetRepresentativeToNames.mapValues {
      case (nameCounts) => nameCounts.toMap
    }.toVector, entityMatchIndices)

    val agentFacetAndNames = agentFacetRepresentativeToNames.toTraversable.flatMap {
      case (agentFacet, nameCounts) =>
        nameCounts.keys.map {
          case name => (agentFacet, name)
        }
    }(scala.collection.breakOut): Vector[(String, String)]

    val termIDFs = computeTermIDFs(agentFacetAndNames.view.map(_._2))

    TextSearchServer[(String, String)] {
      case (agent, name) => (agent, name)
    }.flatMap {
      case textSearchServer =>
        textSearchServer.add(agentFacetAndNames).flatMap {
          case _ =>
            textSearchServer.refreshIndex()
        }.map {
          case _ =>
            textSearchServer
        }
    }.map {
      case textSearchServer =>
        Future.sequence(agentFacetAndNames.map {
          case (agent1, name1) =>
            textSearchServer.search(name1, matchPercent).map {
              case matching =>
                val name1Split = entitySplit(name1)
                matching.collect {
                  case ((agent2, name2), score) if agent1 != agent2 =>
                    (agent1, agent2, name1Split, entitySplit(name2), name1Split)
                }
            }
        }).map(_.flatten).map {
          case matchingCandidates =>
            val result = entityMatching(matchingCandidates, entityMatchingScore(entityMatchingWeightCombine(termIDFs), entityMatchingWeight))
            result
        }
    }
  }

  def entityMatchingScore(combine: (Seq[String], Seq[String], Seq[(Seq[String], Seq[String], Double)]) => Double,
                          entityMatchingWeight: (Seq[String], Seq[String]) => Seq[(Seq[String], Seq[String], Double)])(text1: Seq[String], text2: Seq[String]) = {
    val weight = entityMatchingWeight(text1, text2)
    if (weight.nonEmpty) {
      val weightScore = combine(text1, text2, weight)
      Some((weight, weightScore))
    } else {
      None
    }
  }

  def entityMatching[T, X](matching: Traversable[(X, X, Seq[String], Seq[String], Seq[String])],
                           textMatch: (Seq[String], Seq[String]) => Option[(T, Double)]) = {
    matching.flatMap {
      case (s1, s2, text1, text2, matchText1) =>
        textMatch(text1, text2).map {
          case t => (s1, s2, matchText1, t)
        }
    }
  }

  def computeTermIDFs(content: Traversable[String]) = {
    val normalizedContent = content.map(x => entitySplit(x).map(y => normalizeTerm(y)).distinct).toIndexedSeq
    val N = normalizedContent.size
    val idfs = normalizedContent.flatten.groupBy(identity).mapValues {
      case g => math.log(N / g.size)
    }
    idfs
  }

  def entityMatchingWeightCombine(termIDFs: Map[String, Double]) = (text1: Seq[String], text2: Seq[String], weight: Seq[(Seq[String], Seq[String], Double)]) => {
    weight.map {
      case (terms1, terms2, distance) =>
        terms1.map(termIDFs.compose(normalizeTerm)).sum * (1.0 - distance)
    }.sum / text1.map(termIDFs.compose(normalizeTerm)).sum
  }

  def normalizeTerm(term: String) = {
    Normalization.removeDiacriticalMarks(term).toLowerCase(Locale.ROOT)
  }

  def reconciliateAgentNames(agentNames: Traversable[(String, Map[String, Int])],
                             equivalenceMatching: (Seq[String], Seq[String]) => Seq[(Seq[Int], Seq[Int], Double)]) = {
    agentNames.map {
      case (agent, names) =>
        val weight = names.values.sum
        val splitNames = names.map {
          case (name, count) => (entitySplit(name), count)
        }.toVector
        val reconciliatedNames = reconciliateNames(splitNames, equivalenceMatching).map {
          case (s) =>
            s.map {
              case (tokens, count) =>
                (tokens.map {
                  case token =>
                    if (token.length > 0) {
                      token.head.toString.toUpperCase(Locale.ROOT) + token.tail.toLowerCase(Locale.ROOT)
                    } else {
                      token
                    }
                }, count)
            }.groupBy(_._1).mapValues {
              case x => x.map(_._2).sum
            }.toVector.sortBy(_._2).reverse
        }
        val best = reconciliatedNames.map {
          case x =>
            val s = x.collect {
              case (tokens, count) if tokens.length == 1 => count
            }.sum
            (x.find(_._1.length == 1).map(_._1.mkString(" ")), s)
        }.collect {
          case (Some(token), count) => (token, count)
        }.sortBy(_._2).reverse
        (weight, Vector(best, reconciliatedNames))
    }.toVector.sortBy(_._1)
  }

  def entitySplit(content: String) = {
    tokenSeparator.split(content).toIndexedSeq
  }

  def reconciliateNames(names: Seq[(Seq[String], Int)], comparator: (Seq[String], Seq[String]) => Seq[(Seq[Int], Seq[Int], Double)]) = {
    type INDEX = (Int, Seq[Int])
    val nameMap = new scala.collection.mutable.HashMap[INDEX, Int]
    var idCounter = 0
    def newId() = {
      idCounter += 1
      idCounter
    }
    names.indices.foreach {
      idx1 =>
        val (name1, _) = names(idx1)
        names.indices.drop(idx1 + 1).foreach {
          idx2 =>
            val (name2, _) = names(idx2)
            comparator(name1, name2).foreach {
              case (c1, c2, d) =>
                val v1 = (idx1, c1)
                val v2 = (idx2, c2)
                (nameMap.get(v1), nameMap.get(v2)) match {
                  case (None, None) =>
                    val id = newId()
                    nameMap(v1) = id
                    nameMap(v2) = id
                  case (Some(id), None) =>
                    nameMap(v2) = id
                  case (None, Some(id)) =>
                    nameMap(v1) = id
                  case (Some(id1), Some(id2)) =>
                    if (id1 != id2) {
                      // Detected a corner case.
                      (nameMap.collect {
                        case (v, id) if id == id2 => v
                      }(scala.collection.breakOut): Seq[INDEX]).foreach {
                        case v => nameMap(v) = id1
                      }
                    }
                }
            }

        }
    }
    val usedIndexes = nameMap.keys.flatMap {
      case (idx, c) =>
        c.map((idx, _))
    }.toSet
    names.indices.foreach {
      idx =>
        val (name, _) = names(idx)
        name.indices.foreach {
          case c =>
            if (!usedIndexes.contains((idx, c))) {
              nameMap((idx, Vector(c))) = newId()
            }
        }
    }
    val indexToNameMap = names.indices.flatMap {
      idx =>
        val (name, count) = names(idx)
        name.indices.map {
          case c => (idx, c) ->(name(c), count)
        }
    }.toMap
    nameMap.toVector.groupBy(_._2).map {
      case (id, g) =>
        val equivalentNames = g.map {
          case ((idx, v), _) =>
            val nameParts = v.map {
              case c =>
                indexToNameMap((idx, c))
            }
            val count = nameParts.head._2
            (nameParts.map(_._1), count)
        }
        equivalentNames.sortBy(_._2).reverse
    }.toVector
  }

}
