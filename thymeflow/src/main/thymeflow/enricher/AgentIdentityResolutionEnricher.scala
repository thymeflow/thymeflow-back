package thymeflow.enricher

import java.nio.file.Paths
import java.util.Locale

import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.apache.lucene.search.spell.LevensteinDistance
import org.openrdf.model.impl.SimpleLiteral
import org.openrdf.model.vocabulary.OWL
import org.openrdf.model.{IRI, Resource}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import thymeflow.actors._
import thymeflow.enricher.AgentIdentityResolutionEnricher._
import thymeflow.graph.serialization.GraphML
import thymeflow.graph.{ConnectedComponents, ShortestPath}
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.text.alignment.TextAlignment
import thymeflow.text.distances.BipartiteMatchingDistance
import thymeflow.text.search.elasticsearch.FullTextSearchServer
import thymeflow.text.search.{FullTextSearchPartialTextMatcher, PartialTextMatcher}
import thymeflow.utilities.Memoize
import thymeflow.utilities.text.Normalization

import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
  * @author David Montoya
  */

object AgentIdentityResolutionEnricher {

  sealed trait NamePart

  sealed trait NamePartGraphNode

  sealed trait NamePartMatch

  sealed trait AgentNamePartNode extends NamePartGraphNode {
    def agent: Resource

    def namePart: String
  }

  case class NamePartNoMatch(namePart: String) extends NamePartMatch

  case class NamePartUnqualifiedMatch(namePart: String, matchedNamedPart: String) extends NamePartMatch

  case class NamePartQualifiedMatch(namePart: String, matchedNamePart: String, namePartTypes: Set[IRI]) extends NamePartMatch

  case class TextNamePart(content: String) extends NamePart

  case class VariableNamePart(id: Int) extends NamePart

  case class NamePartNode(content: String) extends NamePartGraphNode

  case class DomainNamePartPatternNamePartNode(domain: String, namePartPattern: IndexedSeq[NamePart], variableNamePart: VariableNamePart) extends NamePartGraphNode

  case class AgentUnqualifiedNamePartNode(agent: Resource, namePart: String) extends AgentNamePartNode

  case class AgentQualifiedNamePartNode(agent: Resource, namePart: String, namePartTypes: Set[IRI]) extends AgentNamePartNode
}

/**
  *
  * Depends on InverseFunctionalPropertyInferencer
  */
class AgentIdentityResolutionEnricher(repositoryConnection: RepositoryConnection, val delay: Duration)
  extends DelayedEnricher with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "AgentIdentityResolution")

  private val metric = new LevensteinDistance()
  private val normalizeTerm = Memoize.concurrentFifoCache(1000, uncachedNormalizeTerm _)
  private val matchingDistance = new BipartiteMatchingDistance(
    (s1, s2) => 1.0d - metric.getDistance(normalizeTerm(s1), normalizeTerm(s2)), 0.3
  )
  private val entityMatchingWeight = matchingDistance.getDistance _
  private val entityMatchIndices = matchingDistance.matchIndices _
  private val parallelism = 1
  private val sameAsThreshold = 0.7d
  // contactRelativeWeight must be between 0 and 1 or None
  private val contactRelativeWeight = Option(0.5)
  private val persistenceThreshold = 0.9d
  // \u2022 is the bullet character
  private val tokenSeparator =
    """[\p{Punct}\s\u2022]+""".r

  private val sameAgentAsQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?agent ?sameAs WHERE {
      ?agent a <${Personal.AGENT}> .
      GRAPH <${Personal.NAMESPACE}inverseFunctionalInferencerOutput> {
        ?agent <${OWL.SAMEAS}> ?sameAs .
      }
    }"""
  )
  private val agentEmailAddressesQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?agent ?emailAddress WHERE {
       ?agent a <${Personal.AGENT}> ;
              <${SchemaOrg.EMAIL}>/<${SchemaOrg.NAME}> ?emailAddress .
    }"""
  )
  private val numberOfMessagesByAgentNameQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?agent ?name (COUNT(?msg) as ?msgCount) WHERE {
      ?agent a <${Personal.AGENT}> ;
               <${SchemaOrg.NAME}> ?name .
      OPTIONAL {
        {
          ?msg <${SchemaOrg.RECIPIENT}> ?agent .
        } UNION {
          ?msg <${SchemaOrg.SENDER}> ?agent .
        }
      }
    } GROUP BY ?agent ?name"""
  )
  private val emailAddressesQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?emailAddressName ?domain ?localPart WHERE {
      ?emailAddress <${Personal.DOMAIN}> ?domain ;
                    <${Personal.LOCAL_PART}> ?localPart ;
                    <${SchemaOrg.NAME}> ?emailAddressName .
    }"""
  )
  private val agentNamePartsQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL,
    s"""SELECT ?agent ?namePartType ?namePart WHERE {
      ?agent a <${Personal.AGENT}> ;
             ?namePartType ?namePart .
      FILTER( ?namePartType = <${SchemaOrg.GIVEN_NAME}> || ?namePartType = <${SchemaOrg.FAMILY_NAME}> )
    }"""
  )

  override protected def runEnrichments() = {
    // get a map assigning to each agent facet its representative in the "shared id" equivalence class
    val agentRepresentativeMap = getSharedIdRepresentativeByAgent
    // get the list email addresses for each agent
    val agentRepresentativeEmailAddresses = getAgentEmails(agentRepresentativeMap.apply)
    // the name counts for each agent
    val (agentRepresentativeContactNames, agentRepresentativeMessageNames) = getAgentNameCounts(agentRepresentativeMap.apply)

    agentNames(agentRepresentativeMessageNames, agentRepresentativeEmailAddresses.apply, agentRepresentativeMap.apply)

    val agentRepresentativeNames = getAgentNameWeights(agentRepresentativeContactNames, agentRepresentativeMessageNames)

    val agentRepresentativeIRIToResourceMap = (agentRepresentativeNames.keys ++ agentRepresentativeEmailAddresses.keys).map {
      case (agent) => (agent.stringValue(), agent)
    }(scala.collection.breakOut): Map[String, Resource]

    // compute a (Agent, Name) list
    val agentAndNames = agentRepresentativeNames.flatMap {
      case (agent, nameCounts) =>
        nameCounts.keys.map {
          case name => (agent, name)
        }
    }(scala.collection.breakOut): Vector[(Resource, String)]

    val termIDFs = computeTermIDFs(agentAndNames.view.map(_._2))
    val textMatchScore = entityMatchingScore(entityMatchingWeightCombine(termIDFs), entityMatchingWeight) _

    FullTextSearchServer[Resource](agentRepresentativeIRIToResourceMap.apply)(_.stringValue()).flatMap {
      case textSearchServer =>
        textSearchServer.add(agentAndNames).flatMap {
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
        val source = Source.fromIterator(() => agentAndNames.iterator).mapAsync(parallelism) {
          case (agent1, name1) =>
            recognizeEntities(entityRecognizer)(Int.MaxValue, clearDuplicateNestedResults = true)(name1).map {
              _.collect({
                case (agent2, name2, name1Split, name2Split, nameMatch) if agent1 != agent2 => (agent1, agent2, name1Split, name2Split, nameMatch)
              })
            }
        }.mapConcat(_.toVector).map {
          case (agent1, agent2, text1, text2, matchText1) =>
            textMatchScore(text1, text2).map {
              case t => (agent1, agent2, matchText1, t)
            }
        }.collect {
          case Some((agent1, agent2, _, (_, similarity))) => (agent1, agent2, similarity)
        }
        buidInclusionRelation(source.filter(_._3 >= sameAsThreshold)).map {
          case equalities =>
            // Filter candidate equalities over a certain threshold in both inclusions
            val sameAsCandidates: Vector[(Resource, Resource)] = equalities.collect {
              case ((agent1, agent2), (leftInclusionWeight, rightInclusionWeight)) if leftInclusionWeight >= sameAsThreshold && rightInclusionWeight >= sameAsThreshold => (agent1, agent2)
            }(scala.collection.breakOut)
            val ordering = implicitly[Ordering[Double]].reverse
            def nameStatements(instance: Resource) = agentRepresentativeNames.get(instance).map(_.toIterable).getOrElse(Iterable.empty)
            // Compute final equality weights
            val equalityWeights = getEqualityWeights(sameAsCandidates, nameStatements, termIDFs, entityMatchingWeight).sortBy(_._5)(ordering)
            // save equalities as owl:sameAs relations in the Repository
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

  def agentNames(agentRepresentativeMessageNames: Map[Resource, Map[String, Long]],
                 agentEmailAddresses: Resource => Traversable[String],
                 getAgentRepresentative: Resource => Resource) = {

    val agentNameParts = getAgentNameParts(getAgentRepresentative)
    val reconciledAgentNames = reconcileAgentNames(agentRepresentativeMessageNames, agentNameParts)
    val agentEmailAddressLocalPartNamePartsAlignment = matchEmailAddressLocalPartWithAgentNames(agentEmailAddresses,
      getAgentEmailAddressParts,
      reconciledAgentNames)
    groupNamePartsByDomain(agentEmailAddressLocalPartNamePartsAlignment)

    resolveNamePartTypes(agentEmailAddressLocalPartNamePartsAlignment)
  }

  private def resolveNamePartTypes(agentEmailAddressLocalPartNamePartsAlignment: IndexedSeq[(Resource, String, String, IndexedSeq[NamePart], Map[VariableNamePart, NamePartMatch], Double, IndexedSeq[(String, Set[IRI])])]) = {
    // build the NamePartGraph
    val domainToAgentNamePartEdges = agentEmailAddressLocalPartNamePartsAlignment.groupBy(x => (x._3.toLowerCase(Locale.ROOT), x._4)).toIndexedSeq.flatMap {
      case ((domain, nameParts), g) =>
        nameParts.flatMap {
          case variableNamePart: VariableNamePart =>
            g.map {
              case (agentRepresentative, _, _, _, variableMap, _, _) =>
                DomainNamePartPatternNamePartNode(domain, nameParts, variableNamePart) ->
                  (variableMap(variableNamePart) match {
                    case NamePartUnqualifiedMatch(_, matchedNamePart) => AgentUnqualifiedNamePartNode(agentRepresentative, matchedNamePart)
                    case NamePartQualifiedMatch(_, matchedNamePart, namePartTypes) => AgentQualifiedNamePartNode(agentRepresentative, matchedNamePart, namePartTypes)
                    case NamePartNoMatch(namePart) => AgentUnqualifiedNamePartNode(agentRepresentative, namePart)
                  })
            }
          case _ =>
            IndexedSeq.empty
        }
    }.toSet
    val agentNamePartToNamePartEdges = domainToAgentNamePartEdges.map(_._2).map {
      node => node -> NamePartNode(node.namePart)
    }
    var id = 0
    val nodes = (domainToAgentNamePartEdges.map(_._1): Set[NamePartGraphNode]) ++ (domainToAgentNamePartEdges.map(_._2): Set[NamePartGraphNode]) ++ (agentNamePartToNamePartEdges.map(_._2): Set[NamePartGraphNode])
    val nodeToNodeIdMap = nodes.map {
      case node =>
        id += 1
        node -> id
    }.toMap
    val nodeIdToNodeMap = nodeToNodeIdMap.map(x => (x._2, x._1))
    val nodeNeighborsMap = (
      domainToAgentNamePartEdges.toIndexedSeq
        ++ domainToAgentNamePartEdges.toIndexedSeq.map(x => (x._2, x._1))
        ++ agentNamePartToNamePartEdges.toIndexedSeq
        ++ agentNamePartToNamePartEdges.toIndexedSeq.map(x => (x._2, x._1))
      ).groupBy(_._1).map {
      case (k, g) => nodeToNodeIdMap(k) -> g.map(x => nodeToNodeIdMap(x._2))
    }.withDefaultValue(IndexedSeq.empty)
    // graph built: (nodes, nodeNeighborsMap, nodeIdToNodeMap, nodeToNodeIdMap)
    // infer name part types
    val agentNamePartsForWhichToInferType = nodes.toIndexedSeq.collect {
      case node: AgentUnqualifiedNamePartNode => node
    }
    val shortestPaths = ShortestPath.allPairsShortestPaths(agentNamePartsForWhichToInferType.map(nodeToNodeIdMap.apply))(node => nodeNeighborsMap(node).map(x => (x, 1)))(from => {
      var bestOption = None: Option[Int]
      (to, w, settled, distanceMap, parent) => {
        bestOption match {
          case Some(best) if w > best => true
          case None =>
            nodeIdToNodeMap(to) match {
              case AgentQualifiedNamePartNode(_, _, namePartTypes) if namePartTypes.nonEmpty =>
                bestOption = Some(w)
                false
              case _ => false
            }
          case _ => false
        }
      }
    })
    val pairOrdering = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = {
        val c = x._1.compareTo(y._1)
        if (c == 0) {
          x._2.compareTo(y._2)
        } else {
          c
        }
      }
    }
    val agentNamePartToNamePartTypes = shortestPaths.collect {
      case (nodeId, (distanceToNodes, _)) =>
        val AgentUnqualifiedNamePartNode(resource, namePart) = nodeIdToNodeMap(nodeId)
        val namePartTypeForNode = distanceToNodes.toIndexedSeq.map(x => (nodeIdToNodeMap(x._1), x._2)).collect {
          case (AgentQualifiedNamePartNode(_, _, namePartTypes), distance) if namePartTypes.nonEmpty =>
            namePartTypes.toIndexedSeq.map((_, distance))
        }.flatten.groupBy(_._1).map {
          case (namePartType, g) =>
            val (_, d) = g.minBy(_._2)
            namePartType ->(-d, g.count(_._2 == d))
        }
        if (namePartTypeForNode.nonEmpty) {
          val M = namePartTypeForNode.maxBy(_._2)(pairOrdering)._2
          (resource, namePart) -> namePartTypeForNode.view.filter(_._2 == M).map(_._1).toIndexedSeq
        } else {
          (resource, namePart) -> IndexedSeq.empty
        }
    }.toMap.withDefaultValue(IndexedSeq.empty)
    // serialize graph with results (debug only)
    serializeAgentNamePartGraph(nodes, domainToAgentNamePartEdges ++ agentNamePartToNamePartEdges, nodeToNodeIdMap.apply, agentNamePartToNamePartTypes.apply)
    agentNamePartToNamePartTypes
  }

  private def serializeAgentNamePartGraph[T](nodes: Set[NamePartGraphNode],
                                             edges: Set[(NamePartGraphNode, NamePartGraphNode)],
                                             nodeId: NamePartGraphNode => Int,
                                             resourceNamePartToNamePartTypes: ((Resource, String)) => Traversable[IRI]) = {
    val nodeIdString = (x: NamePartGraphNode) => s"n${nodeId(x)}"
    var edgeId = 0
    val serializedEdges = edges.toIndexedSeq.map {
      case (from, to) =>
        edgeId += 1
        GraphML.edge(s"e$edgeId", nodeIdString(from), nodeIdString(to))
    }
    val serializedNodes = nodes.toIndexedSeq.map {
      case node@DomainNamePartPatternNamePartNode(domain, nameParts, variableNamePart) =>
        val namePartsSerialization = nameParts.map {
          case v: VariableNamePart => s"<${v.id}>"
          case v: TextNamePart => v.content
        }
        GraphML.node(nodeIdString(node),
          Vector(
            ("type", "domainPattern"),
            ("domain", domain),
            ("nameParts", namePartsSerialization.mkString("")),
            ("variable", variableNamePart.id.toString)
          ))
      case node@AgentUnqualifiedNamePartNode(resource, namePart) =>
        val propertiesSerialization = resourceNamePartToNamePartTypes((resource, namePart)).map(_.getLocalName).toIndexedSeq.sorted.mkString("|")
        GraphML.node(nodeIdString(node),
          Vector(
            ("type", "agentNamePart"),
            ("resource", resource.toString),
            ("namePart", namePart),
            ("properties", s"INF($propertiesSerialization)")
          ))
      case node@AgentQualifiedNamePartNode(resource, namePart, namePartTypes) =>
        val propertiesSerialization = namePartTypes.map(_.getLocalName).toIndexedSeq.sorted.mkString("|")
        GraphML.node(nodeIdString(node),
          Vector(
            ("type", "agentNamePart"),
            ("resource", resource.toString),
            ("namePart", namePart),
            ("properties", propertiesSerialization)
          ))
      case n@NamePartNode(namePart: String) =>
        GraphML.node(nodeIdString(n),
          Vector(
            ("type", "namePart"),
            ("content", namePart)
          ))
    }
    val keys = GraphML.nodeKeys(Vector(
      ("type", "type", "string"),
      ("domain", "domain", "string"),
      ("nameParts", "nameParts", "string"),
      ("variable", "variable", "int"),
      ("resource", "resource", "string"),
      ("namePart", "namePart", "string"),
      ("properties", "properties", "string"),
      ("content", "content", "string")
    ))

    GraphML.write(Paths.get("data/nameParts.graphml"),
      GraphML.graph("nameParts", directed = false, keys, serializedNodes, serializedEdges))
  }

  private def groupNamePartsByDomain(agentEmailAddressLocalPartNamePartsAlignment: IndexedSeq[(Resource, String, String, IndexedSeq[NamePart], Map[VariableNamePart, NamePartMatch], Double, IndexedSeq[(String, Set[IRI])])]) = {
    val intReverseOrdering = implicitly[Ordering[Int]].reverse
    // group by domain (normalized)
    val byDomain = agentEmailAddressLocalPartNamePartsAlignment.groupBy(_._3.toLowerCase(Locale.ROOT)).map {
      case (domain, g) =>
        // group by namePart pattern
        val m = g.groupBy(_._4).map {
          case (nameParts, h) =>
            nameParts ->(h.size, h.map(_._6).sum / h.size.toDouble, h.groupBy(_._5.collect {
              case (k, NamePartQualifiedMatch(_, _, namePartTypes)) => k -> namePartTypes
            }).map {
              case (variablePropertyMap, i) =>
                variablePropertyMap ->(i.size, i.map(_._6).sum / i.size.toDouble, i.groupBy(_._5.collect {
                  case (k, NamePartUnqualifiedMatch(_, _)) => k
                }.toSet).map {
                  case (matchedNameParts, j) =>
                    matchedNameParts ->(j.size, j.map(_._6).sum / j.size.toDouble, j)
                }.toIndexedSeq.sortBy(_._2._1)(intReverseOrdering))
            }.toIndexedSeq.sortBy(_._2._1)(intReverseOrdering))
        }.toIndexedSeq.sortBy(_._2._1)(intReverseOrdering)
        domain ->(g.size, g.map(_._6).sum / g.size.toDouble, m)
    }.toIndexedSeq.sortBy(_._2._1)(intReverseOrdering)
    byDomain
  }

  private def matchEmailAddressLocalPartWithAgentNames(getAgentEmailAddresses: Resource => Traversable[String],
                                                       getEmailAddressParts: String => (String, String),
                                                       reconciledAgentNames: Vector[(Resource, Vector[(String, (Long, Vector[IRI]))], Vector[Vector[(Seq[String], (Long, Vector[IRI]))]])]) = {
    def filter(score: Double, matches: Int, mismatches: Int) = {
      matches.toDouble / (matches + mismatches).toDouble >= 0.7
    }
    reconciledAgentNames.flatMap {
      case (agentRepresentative, primaryNameParts, _) =>
        val agentEmailAddresses = getAgentEmailAddresses(agentRepresentative)
        val normalizedNameParts = primaryNameParts.map {
          case (namePart, (_, namePartTypes)) => (normalizeTerm(namePart), namePartTypes.toSet)
        }
        agentEmailAddresses.map {
          case emailAddress =>
            val (localPart, domain) = getEmailAddressParts(emailAddress)
            val (cost, localPartNamePartsAlignment) = TextAlignment.alignment(normalizedNameParts, normalizeTerm(localPart), filter)(_._1)
            var variableId = 0
            val matchedNamePartsPropertiesMapBuilder = scala.collection.immutable.HashMap.newBuilder[VariableNamePart, NamePartMatch]
            def newVariable() = {
              variableId += 1
              VariableNamePart(variableId)
            }
            def splitTextPart(content: String) = {
              entitySplitParts(content).map {
                case Right(textPart) => TextNamePart(textPart)
                case Left(variablePart) =>
                  val variable = newVariable()
                  matchedNamePartsPropertiesMapBuilder += variable -> NamePartNoMatch(variablePart)
                  variable
              }
            }
            val matchedNameParts = localPartNamePartsAlignment.flatMap {
              case (query, matches) =>
                matches.map((query, _))
            }.sortBy(_._2._2).foldLeft((Vector.empty[NamePart], 0)) {
              case ((s, index), ((nameMatch, properties), (text, from, to))) =>
                val previous =
                  if (index != from) {
                    splitTextPart(localPart.substring(index, from))
                  } else {
                    Vector.empty
                  }
                val variable = newVariable()
                if (properties.nonEmpty) {
                  matchedNamePartsPropertiesMapBuilder += variable -> NamePartQualifiedMatch(text, nameMatch, properties)
                } else {
                  matchedNamePartsPropertiesMapBuilder += variable -> NamePartUnqualifiedMatch(text, nameMatch)
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
            (agentRepresentative,
              localPart,
              domain,
              matchedNameParts: IndexedSeq[NamePart],
              matchedNamePartsPropertiesMapBuilder.result(): Map[VariableNamePart, NamePartMatch],
              cost,
              normalizedNameParts: IndexedSeq[(String, Set[IRI])])
        }
    }
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

  private def reconcileAgentNames(agentRepresentativeMessageNames: Map[Resource, Map[String, Long]],
                                  agentNameParts: Map[Resource, Traversable[(String, IRI)]]) = {
    val agentAndNames = (agentRepresentativeMessageNames.keySet ++ agentNameParts.keySet).view.map {
      case agent =>
        agent ->(agentRepresentativeMessageNames.get(agent).map(_.toIndexedSeq).getOrElse(IndexedSeq.empty), agentNameParts.getOrElse(agent, IndexedSeq.empty))
    }.toIndexedSeq

    reconcileEntityNames(agentAndNames, entityMatchIndices)
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

  /**
    *
    * @return a map [emailAddress -> (localPart, domain)]
    */
  private def getAgentEmailAddressParts = {
    emailAddressesQuery.evaluate().toIterator.map {
      bindingSet =>
        (Option(bindingSet.getValue("emailAddressName")).map(_.stringValue()),
          Option(bindingSet.getValue("localPart")).map(_.stringValue()),
          Option(bindingSet.getValue("domain")).map(_.stringValue()))
    }.collect {
      case (Some(emailAddress), Some(localPart), Some(domain)) => emailAddress ->(localPart, domain)
    }.toMap
  }

  /**
    * Gets a map of agent name parts, for instance:
    * {JohnDoeAgent -> [("John", givenName), ("Doe", familyName)], AliceAgent -> [("Alice", givenName)]}
    *
    * @param getAgentRepresentative a function that assigns to each agent its equivalent class representative
    * @return a map assigning to each Agent a list of (NamePart, NamePartType)
    */
  private def getAgentNameParts(getAgentRepresentative: Resource => Resource) = {
    agentNamePartsQuery.evaluate().toTraversable.map {
      bindingSet =>
        (Option(bindingSet.getValue("agent").asInstanceOf[Resource]),
          Option(bindingSet.getValue("namePartType").asInstanceOf[IRI]),
          Option(bindingSet.getValue("namePart")).map(_.stringValue()))
    }.collect {
      case (Some(agent), Some(namePartType), Some(namePart)) => (getAgentRepresentative(agent), (namePart, namePartType))
    }.groupBy(_._1).map {
      case (agent, g) => agent -> g.map(_._2).toIndexedSeq
    }
  }

  private def getEqualityWeights[RESOURCE](equalityCandidates: Vector[(RESOURCE, RESOURCE)],
                                           nameStatements: (RESOURCE) => Traversable[(String, Double)],
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
                  weight += (count1 * count2) * maxWeight
                  normalization += (count1 * count2)
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
    * @return a map that gives for each agent its equivalent class representative under the "shared id" (email, url...)
    *         equivalence relation
    *         by default, an unknown agent is represented by itself
    */
  private def getSharedIdRepresentativeByAgent: Map[Resource, Resource] = {
    val sameIdAgents = getSameIdAgents
    ConnectedComponents.compute[Resource](sameIdAgents.keys, sameIdAgents.getOrElse(_, None))
      .flatMap(connectedComponent => connectedComponent.map(_ -> connectedComponent.head))
      .toMap
      .withDefault(identity)
  }

  /**
    *
    * @return a map that assigns for each agent its equivalent resources under the "shared id" equivalence relation
    */
  private def getSameIdAgents: Map[Resource, Traversable[Resource]] = {
    sameAgentAsQuery.evaluate().flatMap(bindingSet =>
      (
        Option(bindingSet.getValue("agent").asInstanceOf[Resource]),
        Option(bindingSet.getValue("sameAs").asInstanceOf[Resource])
        ) match {
        case (Some(agent), Some(sameAs)) => Some(agent, sameAs)
        case _ => None
      }
    ).toTraversable.groupBy(_._1).map {
      case (agent1, g) => agent1 -> g.map(_._2)
    }
  }

  /**
    * For each agent, assign weights to each name, by considering the relative weight contact names have versus messages
    * A priori, contact names are more important and the weight of contact names will sum to at least contactRelativeWeight
    * or more if contact names are already more important. If contactRelativeWeight is not set, this procedure simply computes name weights by considering
    * contact name ocurrences just as important as message name ocurrences
    *
    * @param agentRepresentativeContactNames
    * @param agentRepresentativeMessageNames
    * @return a map assigning to each agent a list of names and their weights.
    *         for each agent, all weights sum up to 1.0
    */
  private def getAgentNameWeights(agentRepresentativeContactNames: Map[Resource, Map[String, Long]],
                                  agentRepresentativeMessageNames: Map[Resource, Map[String, Long]]) = {
    (agentRepresentativeContactNames.map((_, true)).view ++ agentRepresentativeMessageNames.map((_, false)).view).toIndexedSeq
      .groupBy(_._1._1).map {
      case (agent, g) =>
        val namesForAgent = g.flatMap {
          case ((_, names), isContactAgent) => names.map {
            case (name, count) => (name, count, isContactAgent)
          }
        }
        val (totalNameCountsForContact, totalNameCountsForMessages) = namesForAgent.view.collect {
          case (_, count, true) => (count, 0L)
          case (_, count, false) => (0L, count)
        }.foldLeft((0L, 0L)) {
          case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)
        }
        assert(totalNameCountsForContact + totalNameCountsForMessages != 0L)
        val s = (totalNameCountsForMessages + totalNameCountsForContact).toDouble
        val (contactMultiplier, messageMultiplier) = contactRelativeWeight match {
          case Some(relativeWeight) =>
            val currentContactWeight = totalNameCountsForContact.toDouble / s
            if (currentContactWeight >= relativeWeight) {
              (1 / s, 1 / s)
            } else {
              (relativeWeight / totalNameCountsForContact.toDouble, (1d - relativeWeight) / totalNameCountsForMessages.toDouble)
            }
          case None =>
            (1 / s, 1 / s)
        }
        agent -> namesForAgent.groupBy(_._1).map {
          case (name, h) =>
            name -> h.collect {
              case (_, count, true) => count.toDouble * contactMultiplier
              case (_, count, false) => count.toDouble
            }.sum
        }
    }
  }

  /**
    *
    * @param getAgentRepresentative a function that assigns to each agent its equivalent class representative
    * @return
    */
  private def getAgentEmails(getAgentRepresentative: Resource => Resource) = {
    agentEmailAddressesQuery.evaluate().toIterator.map(bindingSet =>
      (Option(bindingSet.getValue("agent").asInstanceOf[Resource]), Option(bindingSet.getValue("emailAddress")).map(_.stringValue()))
    ).collect {
      case (Some(agent), Some(emailAddress)) => (getAgentRepresentative(agent), emailAddress)
    }.toTraversable.groupBy(_._1).map {
      case (agentRepresentative, g) => (agentRepresentative, g.map(_._2))
    }.withDefaultValue(Vector.empty)
  }

  /**
    *
    * @param getAgentRepresentative function that assigns to each agent its equivalent class representative
    * @return two maps assigning to each agent a list of names with their ocurrence, for contact cards and message, respectively
    */
  private def getAgentNameCounts(getAgentRepresentative: Resource => Resource) = {
    val agentRepresentativeMessageNamesBuilder = resourceValueOccurrenceCounter[Resource, String]()
    val agentRepresentativeContactNamesBuilder = resourceValueOccurrenceCounter[Resource, String]()

    numberOfMessagesByAgentNameQuery.evaluate().foreach(bindingSet => {
      (
        Option(bindingSet.getValue("agent").asInstanceOf[Resource]),
        Option(bindingSet.getValue("name")).map(_.stringValue()),
        Option(bindingSet.getValue("msgCount")).map(_.asInstanceOf[SimpleLiteral].longValue())
        ) match {
        case (Some(agent), Some(name), Some(msgCount)) =>
          val agentRepresentative = getAgentRepresentative(agent)
          if (msgCount == 0) {
            agentRepresentativeContactNamesBuilder += ((agentRepresentative, name, 1))
          } else {
            agentRepresentativeMessageNamesBuilder += ((agentRepresentative, name, msgCount))
          }
        case _ =>
      }
    })
    val agentRepresentativeContactNames = agentRepresentativeContactNamesBuilder.result()
    val agentRepresentativeMessageNames = agentRepresentativeMessageNamesBuilder.result()
    (agentRepresentativeContactNames, agentRepresentativeMessageNames)
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

  /**
    *
    * @param matching a source stream of (entity1, entity2, similarity) tuples (non-symmetric)
    * @param ordered  an ordering over entities
    * @tparam ENTITY the entity's type
    * @tparam Mat    the stream's materialization value
    * @return a map assigning to each(entity1, entity2) a leftSimilarityValue and a rightSimilarityValue
    */
  private def buidInclusionRelation[ENTITY, Mat](matching: Source[(ENTITY, ENTITY, Double), Mat])
                                                (implicit ordered: ENTITY => Ordered[ENTITY]) = {
    val equalityMapBuilder = new scala.collection.mutable.HashMap[(ENTITY, ENTITY), (Double, Double)]
    matching.runForeach {
      case (entity1, entity2, similarity) =>
        val (key, (newLeftSimilarity, newRightSimilarity)) = if (entity1 > entity2) {
          ((entity2, entity1), (0.0, similarity))
        } else {
          ((entity1, entity2), (similarity, 0.0))
        }
        val (leftSimilarity, rightSimilarity) = equalityMapBuilder.getOrElseUpdate(key, (0.0, 0.0))
        val value = (Math.max(leftSimilarity, newLeftSimilarity), Math.max(rightSimilarity, newRightSimilarity))
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

  private def entitySplit(content: String) = {
    tokenSeparator.split(content).toIndexedSeq
  }

  /**
    *
    * @param resourceNamePartTypes
    * @return the name part type distribution for each name part, as in
    *         [
    *         ("John",10,[(givenName,9),(familyName,1)]),
    *         ("Doe",8,[(givenName,1),(familyName,7)])
    *         ]
    */
  private def namePartTypeDistribution(resourceNamePartTypes: Traversable[((Resource, String), Traversable[IRI])]) = {
    val ordering = implicitly[Ordering[Int]].reverse
    resourceNamePartTypes.groupBy(_._1._2).map {
      case (namePart, g) =>
        val x = g.flatMap(_._2).groupBy(identity).map {
          case (property, h) => property -> h.size
        }.toIndexedSeq.sortBy(_._2)(ordering)
        (namePart, x.map(_._2).sum, x)
    }.toIndexedSeq.sortBy(_._2)(ordering)
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
