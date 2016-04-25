package thymeflow.enricher

import java.util.Locale

import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.apache.lucene.search.spell.LevensteinDistance
import org.openrdf.model.impl.SimpleLiteral
import org.openrdf.model.vocabulary.{OWL, RDF}
import org.openrdf.model.{IRI, Resource}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import pkb.actors._
import pkb.rdf.Converters._
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.utilities.text.Normalization
import thymeflow.graph.ConnectedComponents
import thymeflow.text.alignment.Alignment
import thymeflow.text.distances.BipartiteMatchingDistance
import thymeflow.text.search.elasticsearch.FullTextSearchServer
import thymeflow.text.search.{FullTextSearchPartialTextMatcher, PartialTextMatcher}
import thymeflow.utilities.Memoize

import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
  * @author David Montoya
  */
sealed trait NamePart

case class TextNamePart(content: String) extends NamePart

case class VariableNamePart(id: Int) extends NamePart

class AgentIdentityResolutionEnricher(repositoryConnection: RepositoryConnection, val delay: Duration)
  extends DelayedEnricher with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI("http://thymeflow.com/personal#AgentIdentityResolution")

  private val metric = new LevensteinDistance()
  private val normalizeTerm = Memoize.concurrentFifoCache(1000, uncachedNormalizeTerm _)
  private val matchingDistance = new BipartiteMatchingDistance(
    (s1, s2) => 1.0d - metric.getDistance(normalizeTerm(s1), normalizeTerm(s2)), 0.3
  )
  private val entityMatchingWeight = matchingDistance.getDistance _
  private val entityMatchIndices = matchingDistance.matchIndices _
  private val parallelism = 1
  private val sameAsThreshold = 0.7d
  private val persistenceThreshold = 0.9d
  // \u2022 is the bullet character
  private val tokenSeparator =
    """[\p{Punct}\s\u2022]+""".r

  def runEnrichments() = {

    val agentEmailAddressesQuery =
      s"""
         |SELECT ?agent ?emailAddress
         |WHERE {
         |  ?agent <${SchemaOrg.EMAIL}> ?x .
         |  ?x <${SchemaOrg.NAME}> ?emailAddress .
         | }
      """.stripMargin

    val agentFacetEmailAddresses = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, agentEmailAddressesQuery).evaluate().toIterator.map {
      bindingSet =>
        (Option(bindingSet.getValue("agent").asInstanceOf[Resource]), Option(bindingSet.getValue("emailAddress")).map(_.stringValue()))
    }.collect {
      case (Some(agent), Some(emailAddress)) => (agent, emailAddress)
    }.toVector

    val agentFacetRepresentativeMap = getResourceRepresentatives(agentFacetEmailAddresses)

    val agentEmailAddresses = agentFacetEmailAddresses.map {
      case (agent, emailAddress) => (agentFacetRepresentativeMap(agent), emailAddress)
    }.groupBy(_._1).map {
      case (agentRepresentative, g) => (agentRepresentative, g.map(_._2).distinct)
    }

    val agentFacetRepresentativeMessageNamesBuilder = resourceValueOccurrenceCounter[Resource, String]()
    val agentFacetRepresentativeContactNamesBuilder = resourceValueOccurrenceCounter[Resource, String]()

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

    agentNames(agentFacetRepresentativeMessageNames, agentEmailAddresses, agentFacetRepresentativeMap)

    val contactWeight = 1.0
    val agentFacetRepresentativeNames = (agentFacetRepresentativeContactNames.map((_, contactWeight)) ++ agentFacetRepresentativeMessageNames.map((_, 1.0)))
      .groupBy(_._1._1).map {
      case (agent, g) =>
        val nameWithWeights = g.flatMap(x => x._1._2.map((_, x._2)))
        agent -> nameWithWeights.groupBy(_._1._1).map {
          case (name, h) =>
            name -> h.map(x => (x._2 * x._1._2).toLong).sum
        }
      }

    val agentStringValueToAgent = (agentFacetEmailAddresses.map(_._1) ++ agentFacetRepresentativeNames.keys).map {
      case (agent) => (agent.stringValue(), agent)
    }(scala.collection.breakOut): Map[String, Resource]

    val agentFacetAndNames = agentFacetRepresentativeNames.flatMap {
      case (agentFacet, nameCounts) =>
        nameCounts.keys.map {
          case name => (agentFacet, name)
        }
    }(scala.collection.breakOut): Vector[(Resource, String)]

    val termIDFs = computeTermIDFs(agentFacetAndNames.view.map(_._2))
    val textMatch = entityMatchingScore(entityMatchingWeightCombine(termIDFs), entityMatchingWeight) _

    FullTextSearchServer[Resource](agentStringValueToAgent.apply)(_.stringValue()).flatMap {
      case textSearchServer =>
        textSearchServer.add(agentFacetAndNames).flatMap {
          case _ =>
            textSearchServer.refreshIndex()
        }.map {
          case _ =>
            textSearchServer
        }
    }.flatMap {
      case textSearchServer =>
        val entityRecognizer = FullTextSearchPartialTextMatcher(textSearchServer)
        implicit val orderedValues = (thisValue: Resource) => new Ordered[Resource] {
          override def compare(that: Resource): Int = thisValue.stringValue().compare(that.stringValue())
        }
        val source = Source.fromIterator(() => agentFacetAndNames.iterator).mapAsync(parallelism) {
          case (agent1, name1) =>
            recognizeEntities(entityRecognizer)(Int.MaxValue, clearDuplicateNestedResults = true)(name1).map {
              _.collect({
                case (agent2, name2, name1Split, name2Split, nameMatch) if agent1 != agent2 => ((agent1, name2), (agent2, name2), name1Split, name2Split, nameMatch)
              })
            }
        }.mapConcat(_.toVector).map {
          case (s1, s2, text1, text2, matchText1) =>
            textMatch(text1, text2).map {
              case t => (s1, s2, matchText1, t)
            }
        }.collect {
          case Some(((agent1, _), (agent2, _), _, (_, similarity))) => (agent1, agent2, similarity)
        }
        equalityBuild(source, _ >= sameAsThreshold).map {
          case equalities =>
            // Filter candidate equalities over a certain threshold in both inclusions
            val sameAsCandidates: Vector[(Resource, Resource)] = equalities.collect {
              case ((i1, i2), (w1, w2)) if w1 >= sameAsThreshold && w2 >= sameAsThreshold => (i1, i2)
            }(scala.collection.breakOut)
            val ordering = implicitly[Ordering[Double]].reverse
            def nameStatements(instance: Resource) = agentFacetRepresentativeNames.get(instance).map(_.toIterable).getOrElse(Iterable.empty)
            // Compute final equality weights
            val equalityWeights = getEqualityWeights(sameAsCandidates, nameStatements, termIDFs, entityMatchingWeight).sortBy(_._5)(ordering)
            // Save equalities as owl:sameAs relations in the Repository
            repositoryConnection.begin()
            equalityWeights.foreach {
              case (instance1, _, instance2, _, weight) if weight >= persistenceThreshold =>
                val statement = valueFactory.createStatement(instance1, OWL.SAMEAS, instance2, inferencerContext)
                if (!repositoryConnection.hasStatement(statement, false)) {
                  repositoryConnection.add(statement)
                }
              case _ =>
            }
            repositoryConnection.commit()
        }
    }
  }

  def agentNames(agentFacetRepresentativeMessageNames: Map[Resource, Map[String, Long]],
                 agentEmailAddressMap: Map[Resource, Traversable[String]],
                 agentRepresentativeMap: Map[Resource, Resource]) = {
    val emailAddressesQuery =
      s"""
         |SELECT ?emailAddressName ?domain ?localPart
         |WHERE {
         |  ?emailAddress <${Personal.DOMAIN}> ?domain .
         |  ?emailAddress <${Personal.LOCAL_PART}> ?localPart .
         |  ?emailAddress <${SchemaOrg.NAME}> ?emailAddressName .
         |}
      """.stripMargin

    val emailAddressesDecomposition = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, emailAddressesQuery).evaluate().toIterator.map {
      bindingSet =>
        (Option(bindingSet.getValue("emailAddressName")).map(_.stringValue()),
          Option(bindingSet.getValue("localPart")).map(_.stringValue()),
          Option(bindingSet.getValue("domain")).map(_.stringValue()))
    }.collect {
      case (Some(emailAddress), Some(localPart), Some(domain)) => emailAddress ->(localPart, domain)
    }.toMap

    val agentNamesQuery =
      s"""
         |SELECT ?agent ?nameProperty ?name
         |WHERE {
         |  ?agent ?nameProperty ?name .
         |  FILTER( ?nameProperty = <${SchemaOrg.GIVEN_NAME}> || ?nameProperty = <${SchemaOrg.FAMILY_NAME}> )
         |}
      """.stripMargin

    val agentNameParts = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, agentNamesQuery).evaluate().toIterator.map {
      bindingSet =>
        (Option(bindingSet.getValue("agent").asInstanceOf[Resource]),
          Option(bindingSet.getValue("nameProperty").asInstanceOf[IRI]),
          Option(bindingSet.getValue("name")).map(_.stringValue()))
    }.collect {
      case (Some(agent), Some(nameProperty), Some(name)) => (agentRepresentativeMap.getOrElse(agent, agent), nameProperty, name)
    }.toVector

    val entityPreferredNames = agentNameParts.groupBy(_._1).map {
      case (agent, g) =>
        agent -> g.map {
          case (_, nameProperty, name) => (name, nameProperty)
        }
    }

    val agentWithAllNames = (agentFacetRepresentativeMessageNames.keySet ++ entityPreferredNames.keySet).view.map {
      case agent =>
        agent ->(agentFacetRepresentativeMessageNames.getOrElse(agent, Traversable.empty), entityPreferredNames.getOrElse(agent, Vector.empty))
    }.toVector

    val reconciledAgentNames = reconcileEntityNames(agentWithAllNames, entityMatchIndices)

    def filter(score: Double, matches: Int, mismatches: Int) = {
      matches.toDouble / (matches + mismatches).toDouble >= 0.7
    }
    val agentNameEmailAddressAlignment = reconciledAgentNames.map {
      case (agentRepresentative, g, _) =>
        val emailAddresses = agentEmailAddressMap.getOrElse(agentRepresentative, Vector.empty)
        val names = g.map(x => (x._2._2.toSet, normalizeTerm(x._1)))
        emailAddresses.map {
          case emailAddress =>
            val (localPart, domain) = emailAddressesDecomposition(emailAddress)
            val (cost, alignment) = Alignment.alignment(names, normalizeTerm(localPart), filter)(_._2)
            var variableId = 0
            val variableMap = scala.collection.immutable.HashMap.newBuilder[VariableNamePart, Either[(String, Option[String]), (String, String, Set[IRI])]]
            def newVariable() = {
              variableId += 1
              VariableNamePart(variableId)
            }
            def splitTextPart(content: String) = {
              entitySplitParts(content).map {
                case Right(textPart) => TextNamePart(textPart)
                case Left(variablePart) =>
                  val variable = newVariable()
                  variableMap += variable -> Left((variablePart, None))
                  variable
              }
            }
            val nameParts = alignment.flatMap {
              case (query, matches) =>
                matches.map((query, _))
            }.sortBy(_._2._2).foldLeft((Vector.empty[NamePart], 0)) {
              case ((s, index), ((properties, nameMatch), (text, from, to))) =>
                val previous =
                  if (index != from) {
                    splitTextPart(localPart.substring(index, from))
                  } else {
                    Vector.empty
                  }
                val variable = newVariable()
                if (properties.nonEmpty) {
                  variableMap += variable -> Right((text, nameMatch, properties))
                } else {
                  variableMap += variable -> Left((text, Some(nameMatch)))
                }
                (s ++ (previous :+ variable), to + 1)
            } match {
              case (s, index) =>
                if (index != localPart.length) {
                  s ++ splitTextPart(localPart.substring(index, localPart.length))
                } else {
                  s
                }
            }
            (localPart, domain, nameParts, variableMap.result(), cost, names)
        }
    }
    val byDomain = agentNameEmailAddressAlignment.flatten.groupBy(_._2).map {
      case (domain, g) =>
        val m = g.groupBy(_._3).map {
          case (nameParts, h) =>
            nameParts ->(h.size, h.map(_._5).sum / h.size.toDouble, h.groupBy(_._4.collect {
              case (k, Right((_, _, properties))) => k -> properties
            }).map {
              case (variablePropertyMap, i) =>
                variablePropertyMap ->(i.size, i.map(_._5).sum / i.size.toDouble, i.groupBy(_._4.collect {
                  case (k, Left((_, Some(namePart)))) => k
                }.toSet).map {
                  case (matchedNameParts, j) =>
                    matchedNameParts ->(j.size, j.map(_._5).sum / j.size.toDouble, j)
                }.toIndexedSeq.sortBy(_._2._1).reverse)
            }.toIndexedSeq.sortBy(_._2._1).reverse)
        }.toIndexedSeq.sortBy(_._2._1).reverse
        domain ->(g.size, g.map(_._5).sum / g.size.toDouble, m)
    }.toIndexedSeq.sortBy(_._2._1).reverse
    byDomain
  }

  private def reconcileEntityNames[ENTITY, TERM_PART](entityNames: Traversable[(ENTITY, (Traversable[(String, Long)], Traversable[(String, TERM_PART)]))],
                                                      equivalenceMatching: (Seq[String], Seq[String]) => Seq[(Seq[Int], Seq[Int], Double)]) = {
    val ordering = new Ordering[(Long, Vector[TERM_PART])] {
      override def compare(x: (Long, Vector[TERM_PART]), y: (Long, Vector[TERM_PART])): Int = {
        val (xCount, xTs) = x
        val (yCount, yTs) = y
        val c = yTs.size.compareTo(xTs.size)
        if (c == 0) yCount.compareTo(xCount) else c
      }
    }
    def map(s: Seq[Either[Long, TERM_PART]]) = {
      s.map {
        case Left(count) => (count, Vector.empty)
        case Right(t) => (0L, Vector(t))
      }
    }
    def reduce(s: Seq[(Long, Vector[TERM_PART])]) = {
      s.foldLeft((0L, Vector.empty[TERM_PART])) {
        case ((count1, ts1), (count2, ts2)) =>
          (count1 + count2, ts1 ++ ts2)
      }
    }
    entityNames.map {
      case (agent, (names, nameParts)) =>
        val splitNames = (names.view.map {
          case (name, count) => (entitySplit(name), Left(count))
        } ++ nameParts.map {
          case (name, t) => (entitySplit(name), Right(t))
        }.view).toVector
        val reconciledNames = reconcileNames(splitNames, equivalenceMatching).map {
          case (equivalentNames) =>
            equivalentNames.groupBy(_._1).map {
              case (terms, g) => terms -> reduce(map(g.map(_._2)))
            }.toVector.sortBy(_._2)(ordering)
        }
        val best = reconciledNames.map {
          case x =>
            val s = reduce(x.collect {
              case (terms, countAndT) if terms.length == 1 => countAndT
            })
            (x.find(_._1.length == 1).map(_._1.mkString(" ")), s)
        }.collect {
          case (Some(token), countAndT) => (token, countAndT)
        }.sortBy(_._2)(ordering)
        (agent, best, reconciledNames)
    }.toVector
  }

  private def reconcileNames[TERM, METADATA, SCORE](names: Seq[(Seq[TERM], METADATA)],
                                                    comparator: (Seq[TERM], Seq[TERM]) => Seq[(Seq[Int], Seq[Int], SCORE)]) = {
    type INDEX = (Int, Seq[Int])
    val nameMap = new scala.collection.mutable.HashMap[INDEX, Long]
    var idCounter = 0L
    def newId() = {
      idCounter += 1
      idCounter
    }
    names.indices.foreach {
      nameIndex1 =>
        val (name1, _) = names(nameIndex1)
        names.indices.drop(nameIndex1 + 1).foreach {
          nameIndex2 =>
            val (name2, _) = names(nameIndex2)
            comparator(name1, name2).foreach {
              case (nameTermIndexes1, nameTermIndexes2, d) =>
                val index1 = (nameIndex1, nameTermIndexes1)
                val index2 = (nameIndex2, nameTermIndexes2)
                (nameMap.get(index1), nameMap.get(index2)) match {
                  case (None, None) =>
                    val id = newId()
                    nameMap(index1) = id
                    nameMap(index2) = id
                  case (Some(id), None) =>
                    nameMap(index2) = id
                  case (None, Some(id)) =>
                    nameMap(index1) = id
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
    // compute the set of used (nameIndex, nameTermIndex)
    val usedIndexes = nameMap.keys.flatMap {
      case (nameIndex, nameTermIndexes) =>
        nameTermIndexes.map((nameIndex, _))
    }.toSet
    // create a new id for each unused (nameIndex, nameTermIndex)
    names.indices.foreach {
      index =>
        val (name, _) = names(index)
        name.indices.foreach {
          case c =>
            if (!usedIndexes.contains((index, c))) {
              nameMap((index, Vector(c))) = newId()
            }
        }
    }
    // group indexes by id
    nameMap.toVector.groupBy(_._2).map {
      case (id, g) =>
        val equivalentNames = g.map {
          case ((nameIndex, nameTermIndexes), _) =>
            val (name, count) = names(nameIndex)
            val nameParts = nameTermIndexes.map {
              case nameTermIndex => name(nameTermIndex)
            }
            (nameParts, count)
        }
        equivalentNames
    }.toVector
  }

  private def entitySplitParts(content: String) = {
    var index = 0
    val parts = Vector.newBuilder[Either[String, String]]
    for (m <- tokenSeparator.findAllMatchIn(content)) {
      if (index != m.start) {
        parts += Left(content.substring(index, m.start))
      }
      parts += Right(m.matched)
      index = m.end
    }
    if (index != content.length) {
      parts += Left(content.substring(index, content.length))
    }
    parts.result()
  }

  private def getEqualityWeights[RESOURCE](equalityCandidates: Vector[(RESOURCE, RESOURCE)],
                                           nameStatements: (RESOURCE) => Traversable[(String, Long)],
                                           termIDFs: Map[String, Double],
                                           entityMatchingWeight: (Seq[String], Seq[String]) => Seq[(Seq[String], Seq[String], Double)]) = {
    equalityCandidates.map {
      case (instance1, instance2) =>
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
    }
  }

  private def entityMatchingWeightCombine(termIDFs: Map[String, Double]) = (text1: Seq[String], text2: Seq[String], weight: Seq[(Seq[String], Seq[String], Double)]) => {
    weight.map {
      case (terms1, terms2, distance) =>
        terms1.map(termIDFs.compose(normalizeTerm)).sum * (1.0 - distance)
    }.sum / text1.map(termIDFs.compose(normalizeTerm)).sum
  }

  /**
    * Given a binary relation R over (RESOURCE, VALUE) tuples, computes the RESOURCE equivalence
    * classes imposed by an inverse functionality constraint over R.
    *
    * @param resourceValues the set (RESOURCE, VALUE) tuples of relation R.
    * @tparam RESOURCE the RESOURCE type
    * @tparam VALUE    the VALUE type
    * @return a RESOURCE -> RESOURCE map assigning to each RESOURCE its equivalence class representative
    */
  private def getResourceRepresentatives[RESOURCE, VALUE](resourceValues: Traversable[(RESOURCE, VALUE)]) = {
    val resourceValuesByValue = resourceValues.groupBy(_._2).map { case (value, g) => value -> g.map(_._1) }
    val resourceValuesByResource = resourceValues.groupBy(_._1).map { case (resource, g) => resource -> g.map(_._2) }

    val resourceComponents = ConnectedComponents.compute[Either[RESOURCE, VALUE]](resourceValuesByValue.keys.map(Right(_)), {
      case Left(agent) => resourceValuesByResource.getOrElse(agent, Vector.empty).map(Right.apply)
      case Right(emailAddress) => resourceValuesByValue.getOrElse(emailAddress, Vector.empty).map(Left.apply)
    }).map {
      case component =>
        val resources = component.collect {
          case Left(agent) => agent
        }
        val values = component.collect {
          case Right(emailAddress) => emailAddress
        }
        val representative = resources.head
        (representative, resources, values)
    }

    val resourceRepresentativeMap = resourceComponents.flatMap {
      case (representative, agentFacets, _) =>
        agentFacets.map {
          case (agentFacet) =>
            (agentFacet, representative)
        }
    }.toMap

    resourceRepresentativeMap
  }

  /**
    * Creates VALUE occurrence counter for given RESOURCES
    *
    * @tparam RESOURCE the RESOURCE type
    * @tparam VALUE    the VALUE type
    * @return a builder that returns a map from a RESOURCE to its VALUE counters
    */
  private def resourceValueOccurrenceCounter[RESOURCE, VALUE]() = {
    new mutable.Builder[(RESOURCE, VALUE, Long), Map[RESOURCE, Map[VALUE, Long]]] {
      private val innerMap = new scala.collection.mutable.HashMap[(RESOURCE, VALUE), Long]

      override def +=(elem: (RESOURCE, VALUE, Long)) = elem match {
        case (key, name, count) =>
          val previousCount = innerMap.getOrElse((key, name), 0L)
          innerMap += (key, name) -> (previousCount + count)
          this
      }

      override def result(): Map[RESOURCE, Map[VALUE, Long]] = {
        innerMap.groupBy(_._1._1).map {
          case (agent, g) => agent -> (g.map {
            case ((_, name), count) => (name, count)
          }(scala.collection.breakOut): Map[VALUE, Long])
        }
      }

      override def clear(): Unit = {
        innerMap.clear()
      }
    }: mutable.Builder[(RESOURCE, VALUE, Long), Map[RESOURCE, Map[VALUE, Long]]]
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

  private def equalityBuild[ENTITY, Mat](matching: Source[(ENTITY, ENTITY, Double), Mat],
                                         weightFilter: Double => Boolean)
                                        (implicit ordered: ENTITY => Ordered[ENTITY]) = {
    val equalityMapBuilder = new scala.collection.mutable.HashMap[(ENTITY, ENTITY), (Double, Double)]
    matching.runForeach {
      case (entity1, entity2, weight) =>
        if (weightFilter(weight)) {
          val (key, (new1, new2)) = if (entity1 > entity2) {
            ((entity2, entity1), (0.0, weight))
          } else {
            ((entity1, entity2), (weight, 0.0))
          }
          val (w1, w2) = equalityMapBuilder.getOrElseUpdate(key, (0.0, 0.0))
          val value = (Math.max(w1, new1), Math.max(w2, new2))
          equalityMapBuilder += ((key, value))
        }
    }.map {
      case _ => equalityMapBuilder.result()
    }
  }

  /**
    * Computes term inverse document frequencies (IDFs) from a collection of documents
    *
    * @param documents a collection of documents
    * @return a TERM -> IDF map
    */
  private def computeTermIDFs(documents: Traversable[String]) = {
    val normalizedContent = documents.map(x => entitySplit(x).map(y => normalizeTerm(y)).distinct)
    val N = normalizedContent.size
    val idfs = normalizedContent.flatten.groupBy(identity).map {
      case (term, terms) => term -> math.log(N / terms.size)
    }
    idfs
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

  /** *
    * Normalizes terms by removing their accents (diacritical marks) and changing them to lower case
    *
    * @param term term to normalize
    * @return normalized term
    */
  private def uncachedNormalizeTerm(term: String) = {
    Normalization.removeDiacriticalMarks(term).toLowerCase(Locale.ROOT)
  }

}
