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
import thymeflow.enricher.AgentAttributeIdentityResolutionEnricher._
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

object AgentAttributeIdentityResolutionEnricher {

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
class AgentAttributeIdentityResolutionEnricher(repositoryConnection: RepositoryConnection,
                                               val delay: Duration,
                                               solveDuplicateNameParts: Boolean = false,
                                               solveNamePartTypes: Boolean = false,
                                               debug: Boolean = false)
  extends DelayedEnricher with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "AgentIdentityResolution")
  private val normalizeTerm = Memoize.concurrentFifoCache(1000, uncachedNormalizeTerm _)
  private val termSimilarity = {
    val levensteinDistance = new LevensteinDistance()
    (term1: String, term2: String) => {
      levensteinDistance.getDistance(normalizeTerm(term1), normalizeTerm(term2))
    }
  }

  private val (getMatchingTermSimilarities, getMatchingTermIndices) = {
    val distanceThreshold = 0.3
    val bipartiteMatchingDistance = new BipartiteMatchingDistance((s1, s2) => 1d - termSimilarity(s1, s2), distanceThreshold)
    def getSimilarities(terms1: Seq[String], terms2: Seq[String]) = {
      bipartiteMatchingDistance.getDistance(terms1, terms2).map {
        case (subTerms1, subTerms2, distance) => (subTerms1, subTerms2, 1d - distance)
      }
    }
    (getSimilarities _, bipartiteMatchingDistance.matchIndices _)
  }
  private val parallelism = 1
  private val termInclusionSimilarityThreshold = 0.7d
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
    val agentRepresentativeNames = getAgentNameWeights(agentRepresentativeContactNames, agentRepresentativeMessageNames)
    val agentNameParts = getAgentNameParts(agentRepresentativeMap.apply)
    val reconciledAgentNames = reconcileAgentNames(agentRepresentativeNames, agentNameParts)

    val agentRepresentativeMatchNames = if (solveDuplicateNameParts) {
      val getNamePartTypes = {
        if (solveNamePartTypes) {
          val namePartTypeInference = inferNamePartTypes(reconciledAgentNames, agentRepresentativeEmailAddresses.apply)
          (agent: Resource, primaryName: String, sourceNamePartTypes: Vector[IRI]) => namePartTypeInference((agent, primaryName))
        } else {
          (agent: Resource, primaryName: String, sourceNamePartTypes: Vector[IRI]) => sourceNamePartTypes.toSet
        }
      }
      reconciledAgentNames.map {
        case (agent, primaryNames, _) =>
          agent -> primaryNames.map {
            case (primaryName, (weight, namePartTypes)) => (primaryName, weight, getNamePartTypes(agent, primaryName, namePartTypes))
          }
      }.toMap
    } else {
      agentRepresentativeNames.map {
        case (agent, names) => agent -> names.toIndexedSeq.map {
          case (name, weight) => (name, weight, Set.empty[IRI])
        }
      }
    }.withDefaultValue(IndexedSeq.empty)

    val agentRepresentativeIRIToResourceMap = (agentRepresentativeMatchNames.keys ++ agentRepresentativeEmailAddresses.keys).map {
      case (agent) => (agent.stringValue(), agent)
    }(scala.collection.breakOut): Map[String, Resource]

    // compute a (Agent, Name) list
    val agentAndNames: Vector[(Resource, String)] = if (solveDuplicateNameParts) {
      // in this case, a agent only has one name assignment through agentRepresentativeMatchNames
      // we simply concatenate their name terms sorted by decreasing weight
      val ordering = implicitly[Ordering[Double]].reverse
      agentRepresentativeMatchNames.map {
        case (agent, terms) =>
          val name = terms.sortBy(_._2)(ordering).map(_._1).mkString(" ")
          (agent, name)
      }(scala.collection.breakOut)
    } else {
      agentRepresentativeMatchNames.flatMap {
        case (agent, nameCounts) =>
          nameCounts.map {
            case (name, _, _) => (agent, name)
          }.distinct
      }(scala.collection.breakOut)
    }

    val termIDFs = computeTermIDFs(agentAndNames.view.map(_._2))
    val termMatchingSimilarity = termMatchingScore(normalizedSoftTFIDF(termIDFs), getMatchingTermSimilarities) _
    FullTextSearchServer[Resource](agentRepresentativeIRIToResourceMap.apply)(_.stringValue()).flatMap {
      case textSearchServer =>
        textSearchServer.add(agentAndNames).flatMap {
          _ => textSearchServer.refreshIndex()
        }.map {
          _ => textSearchServer
        }
    }.flatMap {
      case textSearchServer =>
        val entityRecognizer = FullTextSearchPartialTextMatcher(textSearchServer)
        implicit val orderedValues = (thisValue: Resource) => new Ordered[Resource] {
          override def compare(that: Resource): Int = thisValue.stringValue().compare(that.stringValue())
        }
        val similaritySource = Source.fromIterator(() => agentAndNames.iterator).mapAsync(parallelism) {
          case (agent1, name1) =>
            recognizeEntities(entityRecognizer)(Int.MaxValue, clearDuplicateNestedResults = true)(name1).map {
              _.collect({
                case (agent2, name2, terms1, terms2, matchedTerms1) if agent1 != agent2 => (agent1, agent2, terms1, terms2)
              })
            }
        }.mapConcat(_.toVector).map {
          case (agent1, agent2, terms1, terms2) =>
            termMatchingSimilarity(terms1, terms2).map {
              case (_, similarity) => (agent1, agent2, similarity)
            }
        }.collect {
          case Some(x) => x
        }
        // Filter candidate equalities over a certain threshold in both inclusions
        buildInclusionRelationSimilarities(similaritySource.filter(_._3 >= termInclusionSimilarityThreshold)).map {
          case equalities =>
            val sameAsCandidates: Vector[(Resource, Resource)] = equalities.collect {
              case ((agent1, agent2), (leftInclusionWeight, rightInclusionWeight)) if leftInclusionWeight != 0d && rightInclusionWeight != 0d => (agent1, agent2)
            }(scala.collection.breakOut)
            val ordering = implicitly[Ordering[Double]].reverse
            // Compute final equality weights
            val equalityWeights = getEqualityProbabilities(sameAsCandidates, agentRepresentativeMatchNames.apply, termIDFs, getMatchingTermSimilarities).sortBy(_._5)(ordering)
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

  private def inferNamePartTypes[NUMERIC: Numeric](reconciledAgentNames: Vector[(Resource, Vector[(String, (NUMERIC, Vector[IRI]))], Vector[Vector[(Seq[String], (NUMERIC, Vector[IRI]))]])],
                                                   agentEmailAddresses: Resource => Traversable[String]) = {
    val agentEmailAddressLocalPartNamePartsAlignment = matchEmailAddressLocalPartWithAgentNames(agentEmailAddresses,
      getAgentEmailAddressParts,
      reconciledAgentNames)
    if (debug) {
      groupNamePartsByDomain(agentEmailAddressLocalPartNamePartsAlignment)
    }
    resolveNamePartTypes(agentEmailAddressLocalPartNamePartsAlignment).apply _
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
          (resource, namePart) -> namePartTypeForNode.view.filter(_._2 == M).map(_._1).toSet
        } else {
          (resource, namePart) -> Set.empty[IRI]
        }
    }.toMap.withDefaultValue(Set.empty[IRI])
    // serialize graph with results (debug only)
    if (debug) {
      serializeAgentNamePartGraph(nodes, domainToAgentNamePartEdges ++ agentNamePartToNamePartEdges, nodeToNodeIdMap.apply, agentNamePartToNamePartTypes.apply)
    }
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

  private def matchEmailAddressLocalPartWithAgentNames[NUMERIC](getAgentEmailAddresses: Resource => Traversable[String],
                                                                getEmailAddressParts: String => (String, String),
                                                                reconciledAgentNames: Vector[(Resource, Vector[(String, (NUMERIC, Vector[IRI]))], Vector[Vector[(Seq[String], (NUMERIC, Vector[IRI]))]])]) = {
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

  private def reconcileAgentNames[NUMERIC: Numeric](agentRepresentativeMessageNames: Map[Resource, Map[String, NUMERIC]],
                                  agentNameParts: Map[Resource, Traversable[(String, IRI)]]) = {
    val agentAndNames = (agentRepresentativeMessageNames.keySet ++ agentNameParts.keySet).view.map {
      case agent =>
        agent ->(agentRepresentativeMessageNames.get(agent).map(_.toIndexedSeq).getOrElse(IndexedSeq.empty), agentNameParts.getOrElse(agent, IndexedSeq.empty))
    }.toIndexedSeq

    reconcileEntityNames(agentAndNames, getMatchingTermIndices)
  }

  private def reconcileEntityNames[ENTITY, TERM_PART, NUMERIC: Numeric](entityNames: Traversable[(ENTITY, (Traversable[(String, NUMERIC)], Traversable[(String, TERM_PART)]))],
                                                                        equivalenceMatching: (Seq[String], Seq[String]) => Seq[(Seq[Int], Seq[Int], Double)]) = {
    val numeric = implicitly[Numeric[NUMERIC]]
    val ordering = new Ordering[(NUMERIC, Vector[TERM_PART])] {
      override def compare(x: (NUMERIC, Vector[TERM_PART]), y: (NUMERIC, Vector[TERM_PART])): Int = {
        val (xCount, xTs) = x
        val (yCount, yTs) = y
        val c = yTs.size.compareTo(xTs.size)
        if (c == 0) numeric.compare(yCount, xCount) else c
      }
    }
    def map(s: Seq[Either[NUMERIC, TERM_PART]]) = {
      s.map {
        case Left(count) => (count, Vector.empty)
        case Right(t) => (numeric.zero, Vector(t))
      }
    }
    def reduce(s: Seq[(NUMERIC, Vector[TERM_PART])]) = {
      s.foldLeft((numeric.zero, Vector.empty[TERM_PART])) {
        case ((count1, ts1), (count2, ts2)) =>
          (numeric.plus(count1, count2), ts1 ++ ts2)
      }
    }
    entityNames.map {
      case (agent, (names, nameParts)) =>
        val splitNames = (names.view.map {
          case (name, count) => (extractTerms(name), Left(count))
        } ++ nameParts.map {
          case (name, t) => (extractTerms(name), Right(t))
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

  private def getEqualityProbabilities[RESOURCE](equalityCandidates: Vector[(RESOURCE, RESOURCE)],
                                                 nameStatements: (RESOURCE) => Traversable[(String, Double, Set[IRI])],
                                                 termIDFs: Map[String, Double],
                                                 entityMatchingWeight: (Seq[String], Seq[String]) => Seq[(Seq[String], Seq[String], Double)]) = {
    equalityCandidates.map {
      case (instance1, instance2) =>
        val instance1Names = nameStatements(instance1)
        val instance2Names = nameStatements(instance2)
        val probability = getNamesEqualityProbability(instance1Names, instance2Names, termIDFs, entityMatchingWeight)
        (instance1, instance1Names, instance2, instance2Names, probability)
    }
  }

  private def getNamesEqualityProbability[RESOURCE](names1: Traversable[(String, Double, Set[IRI])],
                                                    names2: Traversable[(String, Double, Set[IRI])],
                                                    termIDFs: Map[String, Double],
                                                    termSimilarityMatch: (Seq[String], Seq[String]) => Seq[(Seq[String], Seq[String], Double)]) = {
    var weight = 0d
    var normalization = 0d
    names1.foreach {
      case (name1, count1, _) =>
        names2.foreach {
          case (name2, count2, _) =>
            val terms1 = extractTerms(name1)
            val terms2 = extractTerms(name2)
            if (terms1.nonEmpty && terms2.nonEmpty) {
              val similarities1 = termSimilarityMatch(terms1, terms2)
              val similarities2 = similarities1.map {
                case (s1, s2, w) => (s2, s1, w)
              }
              val maxWeight = scala.math.max(normalizedSoftTFIDF(termIDFs)(terms1, terms2, similarities1), normalizedSoftTFIDF(termIDFs)(terms2, terms1, similarities2))
              weight += (count1 * count2) * maxWeight
              normalization += (count1 * count2)
            }
        }
    }
    val equalityProbability = if (normalization != 0.0) {
      weight / normalization
    } else {
      0.0
    }
    equalityProbability
  }

  private def normalizedSoftTFIDF(termIDFs: Map[String, Double]) = (text1: Seq[String], text2: Seq[String], similarities: Seq[(Seq[String], Seq[String], Double)]) => {
    val denominator = text1.map(termIDFs.compose(normalizeTerm)).sum * text2.map(termIDFs.compose(normalizeTerm)).sum
    if (denominator == 0d) {
      0d
    } else {
      similarities.map {
        case (terms1, terms2, similarity) =>
          terms1.map(termIDFs.compose(normalizeTerm)).sum * terms2.map(termIDFs.compose(normalizeTerm)).sum * similarity
      }.sum / denominator
    }
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
    * @param agentRepresentativeContactNames a map from an agent representative to a name occurrence map, extracted from contact cards
    * @param agentRepresentativeMessageNames a map from an agent representative to a name occurrence map, extracted from messages
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
        val nameWeights = namesForAgent.groupBy(_._1).map {
          case (name, h) =>
            name -> h.collect {
              case (_, count, true) => count.toDouble * contactMultiplier
              case (_, count, false) => count.toDouble * messageMultiplier
            }.sum
        }
        agent -> nameWeights
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

  private def termMatchingScore(combineWeights: (Seq[String], Seq[String], Seq[(Seq[String], Seq[String], Double)]) => Double,
                                getMatchingWeights: (Seq[String], Seq[String]) => Seq[(Seq[String], Seq[String], Double)])(terms1: Seq[String], terms2: Seq[String]) = {
    val matchingWeights = getMatchingWeights(terms1, terms2)
    if (matchingWeights.nonEmpty) {
      val combinedWeight = combineWeights(terms1, terms2, matchingWeights)
      Some((matchingWeights, combinedWeight))
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
  private def buildInclusionRelationSimilarities[ENTITY, Mat](matching: Source[(ENTITY, ENTITY, Double), Mat])
                                                             (implicit ordered: ENTITY => Ordered[ENTITY]) = {
    val inclusionRelationSimilarityMapBuilder = new scala.collection.mutable.HashMap[(ENTITY, ENTITY), (Double, Double)]
    matching.runForeach {
      case (entity1, entity2, similarity) =>
        val (key, (newLeftSimilarity, newRightSimilarity)) = if (entity1 > entity2) {
          ((entity2, entity1), (0.0, similarity))
        } else {
          ((entity1, entity2), (similarity, 0.0))
        }
        val (leftSimilarity, rightSimilarity) = inclusionRelationSimilarityMapBuilder.getOrElseUpdate(key, (0.0, 0.0))
        val value = (Math.max(leftSimilarity, newLeftSimilarity), Math.max(rightSimilarity, newRightSimilarity))
        inclusionRelationSimilarityMapBuilder += key -> value
    }.map {
      case _ => inclusionRelationSimilarityMapBuilder.result()
    }
  }

  /**
    * Computes term inverse document frequencies (IDFs) from a collection of documents
    *
    * @param documents a collection of documents
    * @return a TERM -> IDF map
    */
  private def computeTermIDFs(documents: Traversable[String]) = {
    val normalizedContent = documents.map(x => extractTerms(x).map(y => normalizeTerm(y)).distinct)
    val N = normalizedContent.size
    val idfs = normalizedContent.flatten.groupBy(identity).map {
      case (term, terms) => term -> math.log(N / terms.size)
    }
    idfs
  }

  private def recognizeEntities[T](entityRecognizer: PartialTextMatcher[T])
                                  (searchDepth: Int = 3,
                                   clearDuplicateNestedResults: Boolean = false)(text1: String) = {
    val terms1 = extractTerms(text1)
    entityRecognizer.partialMatchQuery(terms1, searchDepth, clearDuplicateNestedResults = clearDuplicateNestedResults).map {
      case (positions) =>
        positions.flatMap {
          case (position, matchedEntities) =>
            val matchedTerms1 = position.slice(terms1)
            matchedEntities.map {
              case (matchedEntity2, matchedText2, _) =>
                (matchedEntity2, matchedText2, terms1, extractTerms(matchedText2), matchedTerms1)
            }
        }
    }
  }

  /**
    *
    * @param content to extract terms from
    * @return a list of extracted terms, in their order of appearance
    */
  private def extractTerms(content: String) = {
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
