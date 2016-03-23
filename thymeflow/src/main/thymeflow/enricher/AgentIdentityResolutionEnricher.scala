package thymeflow.enricher

import java.util.Locale

import com.typesafe.scalalogging.StrictLogging
import org.apache.lucene.search.spell.LevensteinDistance
import org.openrdf.model.Resource
import org.openrdf.model.impl.SimpleLiteral
import org.openrdf.model.vocabulary.{OWL, RDF}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import pkb.actors._
import pkb.rdf.Converters._
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.utilities.text.Normalization
import thymeflow.graph.ConnectedComponents
import thymeflow.text.distances.BipartiteMatchingDistance
import thymeflow.text.search.elasticsearch.FullTextSearchServer
import thymeflow.text.search.{FullTextSearchPartialTextMatcher, PartialTextMatcher}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * @author David Montoya
  */
class AgentIdentityResolutionEnricher(repositoryConnection: RepositoryConnection, val delay: Duration)
  extends DelayedEnricher with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI("http://thymeflow.com/personal#agentIdentityResolution")

  // \u2022 is the bullet character
  private val tokenSeparator =
    """[\p{Punct}\s\u2022]+""".r

  def runEnrichments() = {
    val metric = new LevensteinDistance()
    val sameAsThreshold = 0.7d
    val persistenceThreshold = 0.9d
    val distance = new BipartiteMatchingDistance(
      (s1, s2) => 1.0d - metric.getDistance(normalizeTerm(s1), normalizeTerm(s2)), 0.3
    )
    val entityMatchingWeight = distance.getDistance _
    val entityMatchIndices = distance.matchIndices _


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
        (Option(bindingSet.getValue("agent").asInstanceOf[Resource]), Option(bindingSet.getValue("emailAddress")).map(_.stringValue()))
    }.collect {
      case (Some(agent), Some(emailAddress)) => (agent, emailAddress)
    }

    val agentFacetEmailAddressesByEmailAddress = agentFacetEmailAddresses.groupBy(_._2).mapValues(_.map(_._1))
    val agentFacetEmailAddressesByAgentFacet = agentFacetEmailAddresses.groupBy(_._1).mapValues(_.map(_._2))

    val emailAgentFacetComponents = ConnectedComponents.compute[Either[Resource, String]](agentFacetEmailAddressesByEmailAddress.keys.map(Right(_)), {
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

    val messageFilter =
      s"""
         |  {
         |    ?msg <${Personal.PRIMARY_RECIPIENT}> ?agent .
         |  }UNION{
         |    ?msg <${Personal.BLIND_COPY_RECIPIENT}> ?agent .
         |  }UNION{
         |    ?msg <${Personal.COPY_RECIPIENT}> ?agent .
         |  }UNION{
         |    ?msg <${SchemaOrg.AUTHOR}> ?agent .
         |  }
      """.stripMargin

    val agentNamesInMessagesQuery =
      s"""
         |SELECT ?agent ?name (COUNT(?msg) as ?msgCount)
         |WHERE {
         |  ?agent <${SchemaOrg.NAME}> ?name .
         |  ?agent <${RDF.TYPE}> <${Personal.AGENT}> .
         |  $messageFilter
         | } GROUP BY ?agent ?name
      """.stripMargin

    def nameCountsBuilder[T]() = {
      new mutable.Builder[(T, String, Long), Map[T, Map[String, Long]]] {
        private val innerMap = new scala.collection.mutable.HashMap[(T, String), Long]

        override def +=(elem: (T, String, Long)) = elem match {
          case (key, name, count) =>
            val previousCount = innerMap.getOrElse((key, name), 0L)
            innerMap += (key, name) -> (previousCount + count)
            this
        }

        override def result(): Map[T, Map[String, Long]] = {
          innerMap.toTraversable.groupBy(_._1._1).mapValues {
            case (g) => g.map {
              case ((_, name), count) => (name, count)
            }(scala.collection.breakOut)
          }
        }

        override def clear(): Unit = {
          innerMap.clear()
        }
      }
    }

    val agentFacetRepresentativeMessageNamesBuilder = nameCountsBuilder[Resource]()
    val agentFacetRepresentativeContactNamesBuilder = nameCountsBuilder[Resource]()

    repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, agentNamesInMessagesQuery).evaluate().foreach {
      bindingSet =>
        val (agentOption, nameOption, msgCountOption) = (Option(bindingSet.getValue("agent").asInstanceOf[Resource]), Option(bindingSet.getValue("name")).map(_.stringValue()), Option(bindingSet.getValue("msgCount")).map(_.asInstanceOf[SimpleLiteral]))
        (agentOption, nameOption, msgCountOption) match {
          case (Some(agent), Some(name), Some(msgCount)) =>
            val agentRepresentative = agentFacetRepresentativeMap.getOrElse(agent, agent)
            agentFacetRepresentativeMessageNamesBuilder += ((agentRepresentative, name, msgCount.longValue()))
          case _ =>
        }
    }

    val agentNamesNotInMessagesQuery =
      s"""
         |SELECT ?agent ?name
         |WHERE {
         |  ?agent <${SchemaOrg.NAME}> ?name .
         |  ?agent <${RDF.TYPE}> <${Personal.AGENT}> .
         |  FILTER NOT EXISTS { $messageFilter }
         | }
      """.stripMargin

    repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, agentNamesNotInMessagesQuery).evaluate().foreach {
      bindingSet =>
        val (agentOption, nameOption) = (Option(bindingSet.getValue("agent").asInstanceOf[Resource]), Option(bindingSet.getValue("name")).map(_.stringValue()))
        (agentOption, nameOption) match {
          case (Some(agent), Some(name)) =>
            val agentRepresentative = agentFacetRepresentativeMap.getOrElse(agent, agent)
            agentFacetRepresentativeContactNamesBuilder += ((agentRepresentative, name, 1))
          case _ =>
        }
    }
    val agentFacetRepresentativeContactNames = agentFacetRepresentativeContactNamesBuilder.result()
    val agentFacetRepresentativeMessageNames = agentFacetRepresentativeMessageNamesBuilder.result()

    val contactWeight = 1.0
    val agentFacetRepresentativeNames = (agentFacetRepresentativeContactNames.toTraversable.map((_, contactWeight))
      ++ agentFacetRepresentativeMessageNames.toTraversable.map((_, 1.0))).groupBy(_._1._1).mapValues {
      case g => g.flatMap(x => x._1._2.toTraversable.map((_, x._2))).groupBy(_._1._1).mapValues {
        case h => h.map(x => (x._2 * x._1._2).toLong).sum
      }
    }

    val agentStringValueToAgent = (agentFacetEmailAddresses.toTraversable.map(_._1) ++ agentFacetRepresentativeNames.toTraversable.map(_._1)).map {
      case (agent) => (agent.stringValue(), agent)
    }(scala.collection.breakOut): Map[String, Resource]

    val reconciliatedAgentNames = reconciliateAgentNames(agentFacetRepresentativeMessageNames, agentFacetRepresentativeContactNames, entityMatchIndices)

    val agentFacetAndNames = agentFacetRepresentativeNames.toTraversable.flatMap {
      case (agentFacet, nameCounts) =>
        nameCounts.keys.map {
          case name => (agentFacet, name)
        }
    }(scala.collection.breakOut): Vector[(Resource, String)]

    val termIDFs = computeTermIDFs(agentFacetAndNames.view.map(_._2))


    FullTextSearchServer[Resource](agentStringValueToAgent.apply)(_.stringValue()).flatMap {
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
        val entityRecognizer = FullTextSearchPartialTextMatcher(textSearchServer)
        Future.sequence(agentFacetAndNames.map {
          case (agent1, name1) =>
            recognizeEntities(entityRecognizer)(Int.MaxValue, clearDuplicateNestedResults = true)(name1).map {
              _.collect({
                case (agent2, name2, name1Split, name2Split, nameMatch) if agent1 != agent2 => ((agent1, name2), (agent2, name2), name1Split, name2Split, nameMatch)
              })
            }
        }).map(_.flatten).map {
          case matchingCandidates =>
            val matchedEntities = entityMatching(matchingCandidates, entityMatchingScore(entityMatchingWeightCombine(termIDFs), entityMatchingWeight))
            implicit val orderedValues = (thisValue: Resource) => new Ordered[Resource] {
              override def compare(that: Resource): Int = thisValue.stringValue().compare(that.stringValue())
            }
            equalityBuild(matchedEntities.view.map { case x => (x._1._1, x._2._1, x._4._2) })
        }.map {
          case equalities =>
            val sameAsCandidates = equalities.collect {
              case ((i1, i2), (w1, w2)) if w1 >= sameAsThreshold && w2 >= sameAsThreshold => (i1, i2)
            }
            val ordering = implicitly[Ordering[Double]].reverse
            val equalityWeights = sameAsCandidates.map {
              case (instance1, instance2) =>
                def nameStatements(instance: Resource) = {
                  agentFacetRepresentativeNames.get(instance).map(_.toIterable).getOrElse(Iterable.empty)
                }
                var weight = 0d
                var normalization = 0d
                val nameStatements1 = nameStatements(instance1)
                val nameStatements2 = nameStatements(instance2)
                nameStatements1.foreach {
                  case (name1, count1) =>
                    nameStatements2.foreach {
                      case (name2, count2) =>
                        val split1 = entitySplit(name1)
                        val split2 = entitySplit(name2)
                        if (split1.nonEmpty && split2.nonEmpty) {
                          val weight1 = entityMatchingWeight(split1, split2)
                          val weight2 = weight1.map {
                            case (s1, s2, w) => (s2, s1, w)
                          }
                          val maxWeight = scala.math.max(entityMatchingWeightCombine(termIDFs)(split1, split2, weight1), entityMatchingWeightCombine(termIDFs)(split2, split1, weight2))
                          weight += (count1 * count2).toDouble * maxWeight
                          normalization += (count1 * count2).toDouble
                        }
                    }
                }
                val equalityMatchingWeight = if (normalization != 0.0) {
                  weight / normalization
                } else {
                  0.0
                }
                (instance1, nameStatements1, instance2, nameStatements2, equalityMatchingWeight)
            }.toIndexedSeq.sortBy(_._5)(ordering)

            repositoryConnection.begin()
            equalityWeights.foreach {
              case (instance1, _, instance2, _, weight) if weight >= persistenceThreshold =>
                val statement = valueFactory.createStatement(instance1, OWL.SAMEAS, instance2, inferencerContext)
                repositoryConnection.add(statement)
              case _ =>
            }
            repositoryConnection.commit()
        }
    }
  }

  private def entityMatchingScore(combine: (Seq[String], Seq[String], Seq[(Seq[String], Seq[String], Double)]) => Double,
                                  entityMatchingWeight: (Seq[String], Seq[String]) => Seq[(Seq[String], Seq[String], Double)])(text1: Seq[String], text2: Seq[String]) = {
    val weight = entityMatchingWeight(text1, text2)
    if (weight.nonEmpty) {
      val weightScore = combine(text1, text2, weight)
      Some((weight, weightScore))
    } else {
      None
    }
  }

  private def equalityBuild[T](matching: Traversable[(T, T, Double)])(implicit ordered: T => Ordered[T]) = {
    val equalityMapBuilder = new scala.collection.mutable.HashMap[(T, T), (Double, Double)]
    def addEquality(instance1: T, instance2: T, weight: Double) {
      val (key, (new1, new2)) = if (instance1 > instance2) {
        ((instance2, instance1), (0.0, weight))
      } else {
        ((instance1, instance2), (weight, 0.0))
      }
      val (w1, w2) = equalityMapBuilder.getOrElseUpdate(key, (0.0, 0.0))
      val value = (Math.max(w1, new1), Math.max(w2, new2))
      equalityMapBuilder += ((key, value))
    }
    matching.foreach {
      case (instance1, instance2, weight) =>
        addEquality(instance1, instance2, weight)
    }
    equalityMapBuilder.result()
  }

  private def entityMatching[T, X](matching: Traversable[(X, X, Seq[String], Seq[String], Seq[String])],
                                   textMatch: (Seq[String], Seq[String]) => Option[(T, Double)]) = {
    matching.flatMap {
      case (s1, s2, text1, text2, matchText1) =>
        textMatch(text1, text2).map {
          case t => (s1, s2, matchText1, t)
        }
    }
  }

  private def computeTermIDFs(content: Traversable[String]) = {
    val normalizedContent = content.map(x => entitySplit(x).map(y => normalizeTerm(y)).distinct).toIndexedSeq
    val N = normalizedContent.size
    val idfs = normalizedContent.flatten.groupBy(identity).mapValues {
      case g => math.log(N / g.size)
    }
    idfs
  }

  private def normalizeTerm(term: String) = {
    Normalization.removeDiacriticalMarks(term).toLowerCase(Locale.ROOT)
  }

  private def entityMatchingWeightCombine(termIDFs: Map[String, Double]) = (text1: Seq[String], text2: Seq[String], weight: Seq[(Seq[String], Seq[String], Double)]) => {
    weight.map {
      case (terms1, terms2, distance) =>
        terms1.map(termIDFs.compose(normalizeTerm)).sum * (1.0 - distance)
    }.sum / text1.map(termIDFs.compose(normalizeTerm)).sum
  }

  private def reconciliateAgentNames[T](agentNames: Traversable[(T, Map[String, Long])],
                                        agentPreferredNames: Map[T, Map[String, Long]],
                                        equivalenceMatching: (Seq[String], Seq[String]) => Seq[(Seq[Int], Seq[Int], Double)]) = {
    val ordering = implicitly[Ordering[Long]].reverse
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
                    if (token.nonEmpty) {
                      token.head.toString.toUpperCase(Locale.ROOT) + token.tail.toLowerCase(Locale.ROOT)
                    } else {
                      token
                    }
                }, count)
            }.groupBy(_._1).mapValues {
              case x => x.map(_._2).sum
            }.toVector.sortBy(_._2)(ordering)
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
        (weight, agentPreferredNames.getOrElse(agent, Map.empty), Vector(best, reconciliatedNames))
    }.toVector.sortBy(_._1)(ordering)
  }

  private def reconciliateNames(names: Seq[(Seq[String], Long)],
                                comparator: (Seq[String], Seq[String]) => Seq[(Seq[Int], Seq[Int], Double)]) = {
    type INDEX = (Int, Seq[Int])
    val nameMap = new scala.collection.mutable.HashMap[INDEX, Long]
    var idCounter = 0L
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

  private def entitySplit(content: String) = {
    tokenSeparator.split(content).toIndexedSeq
  }

  private def recognizeEntities[T](entityRecognizer: PartialTextMatcher[T])
                                  (searchDepth: Int = 3,
                                   clearDuplicateNestedResults: Boolean = false)(value1: String) = {
    val value1Split = entitySplit(value1)
    entityRecognizer.partialMatchQuery(value1Split, searchDepth, clearDuplicateNestedResults = clearDuplicateNestedResults).map {
      case (positions) =>
        positions.flatMap {
          case (position, matchedEntities) =>
            val literal1Slice = position.slice(value1Split)
            matchedEntities.map {
              case (matchedEntity2, matchedValue2, _) =>
                (matchedEntity2, matchedValue2, value1Split, entitySplit(matchedValue2), literal1Slice)
            }
        }
    }
  }

}
