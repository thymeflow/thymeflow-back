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
import thymeflow.rdf.model.ModelDiff
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.text.alignment.TextAlignment
import thymeflow.text.distances.BipartiteMatchingDistance
import thymeflow.text.search.elasticsearch.FullTextSearchServer
import thymeflow.utilities.Memoize
import thymeflow.utilities.text.Normalization

import scala.collection.mutable
import scala.concurrent.Await
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
                                               solveDuplicateNameParts: Boolean = false,
                                               solveNamePartTypes: Boolean = false,
                                               debug: Boolean = false) extends Enricher with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "AgentIdentityResolution")
  private val normalizeTerm = Memoize.concurrentFifoCache(1000, uncachedNormalizeTerm _)
  private val termSimilarity = {
    val levensteinDistance = new LevensteinDistance()
    (term1: String, term2: String) => {
      levensteinDistance.getDistance(normalizeTerm(term1), normalizeTerm(term2))
    }
  }

  private val (getMatchingTermSimilarities, getMatchingTermIndicesWithSimilarities, getMatchingTermIndices) = {
    val distanceThreshold = 0.3
    val bipartiteMatchingDistance = new BipartiteMatchingDistance((s1, s2) => 1d - termSimilarity(s1, s2), distanceThreshold)
    def getIndicesWithSimilarities(terms1: IndexedSeq[String], terms2: IndexedSeq[String]) = {
      bipartiteMatchingDistance.matchIndices(terms1, terms2).map {
        case (subTerms1, subTerms2, distance) => (subTerms1, subTerms2, 1d - distance)
      }
    }
    def getSimilarities(terms1: IndexedSeq[String], terms2: IndexedSeq[String]) = {
      bipartiteMatchingDistance.getDistance(terms1, terms2).map {
        case (subTerms1, subTerms2, distance) => (subTerms1, subTerms2, 1d - distance)
      }
    }
    def getIndices(terms1: IndexedSeq[String], terms2: IndexedSeq[String]) = {
      bipartiteMatchingDistance.matchIndices(terms1, terms2).map {
        case (subTerms1, subTerms2, distance) => (subTerms1, subTerms2)
      }
    }
    (getSimilarities _, getIndicesWithSimilarities _, getIndices _)
  }
  private val searchMatchPercent = 70
  private val searchSize = 10000
  private val parallelism = 2
  // contactRelativeWeight must be between 0 and 1 or None
  private val contactRelativeWeight = Option(0.5)
  private val persistenceThreshold = 0.4d
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

  override def enrich(diff: ModelDiff): Unit = {
    // get a map assigning to each agent facet its representative in the "shared id" equivalence class
    val agentRepresentativeMap = getSharedIdRepresentativeByAgent
    // get the list email addresses for each agent
    val agentRepresentativeEmailAddresses = getAgentEmails(agentRepresentativeMap.apply)
    // for each agent, get a count for each name
    val (agentRepresentativeContactNames, agentRepresentativeMessageNames) = getAgentNameCounts(agentRepresentativeMap.apply)
    // for each agent, get a weight for each name, taking into account the relative weight of contact card vs messages
    val agentRepresentativeNames = getAgentNameWeights(agentRepresentativeContactNames, agentRepresentativeMessageNames)
    // for each agent, get a weight for each name, and possibly name term types
    val agentRepresentativeMatchNames =
      if (solveDuplicateNameParts) {
        val agentNameParts = getAgentNameParts(agentRepresentativeMap.apply)
        val reconciledAgentNames = reconcileAgentNames(agentRepresentativeNames, agentNameParts)
        val getNamePartTypes = {
          if (solveNamePartTypes) {
            val namePartTypeInference = inferNamePartTypes(reconciledAgentNames, agentRepresentativeEmailAddresses.apply)
            (agent: Resource, primaryName: String, sourceNamePartTypes: IndexedSeq[IRI]) => namePartTypeInference((agent, primaryName))
          } else {
            (agent: Resource, primaryName: String, sourceNamePartTypes: IndexedSeq[IRI]) => sourceNamePartTypes.toSet
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

    // compute a (Agent, NameTerms) list
    val agentAndNormalizedNameTerms: scala.collection.immutable.IndexedSeq[(Resource, IndexedSeq[(String, Double)])] =
      if (solveDuplicateNameParts) {
        // in this case, a agent only has one name assignment through agentRepresentativeMatchNames
        agentRepresentativeMatchNames.map {
          case (agent, terms) =>
            (agent, terms.map {
              case (term, weight, _) => (normalizeTerm(term), weight)
            })
        }(scala.collection.breakOut)
      } else {
        agentRepresentativeMatchNames.map {
          case (agent, nameCounts) =>
            agent -> nameCounts.flatMap {
              case (name, weight, _) =>
                extractTerms(name).map(normalizeTerm).map((_, weight))
            }.groupBy(_._1).map {
              case (term, occurrences) => term -> Math.min(occurrences.map(_._2).sum, 1d)
            }.toIndexedSeq
        }(scala.collection.breakOut)
      }

    // compute inverse document frequency of each term, when considering each agent as a document, where each term has a probability to be in the document.
    val termIDFs = computeTermIDFs(agentAndNormalizedNameTerms.view.map(_._2)).compose(normalizeTerm)

    // a map from an agent's stringValue to the agent (for deserialization)
    val agentRepresentativeIRIToResourceMap = (agentRepresentativeMatchNames.keys ++ agentRepresentativeEmailAddresses.keys).map {
      case (agent) =>
        (agent.stringValue(), agent)
    }(scala.collection.breakOut): Map[String, Resource]

    // compute an agent -> termSet map
    val normalizedTermsToAgents = agentAndNormalizedNameTerms.view.flatMap {
      case (agent, nameTerms) =>
        nameTerms.map {
          case (nameTerm, _) => (nameTerm, agent)
        }
    }.groupBy(_._1).map {
      case (term, agents) => term -> agents.map(_._2).toSet
    }

    val result = FullTextSearchServer[Resource](agentRepresentativeIRIToResourceMap.apply)(_.stringValue(), searchSize = searchSize).flatMap {
      case textSearchServer =>
        // add all terms to the index
        textSearchServer.add(agentAndNormalizedNameTerms.view.flatMap {
          case (agent, nameTerms) => nameTerms.map {
            case (nameTerm, _) => (agent, nameTerm)
          }
        }).flatMap {
          _ => textSearchServer.refreshIndex()
        }.map {
          _ => textSearchServer
        }
    }.flatMap {
      case textSearchServer =>
        // an order on agents (comparison of their string value)
        implicit val orderedAgents = (thisValue: Resource) => new Ordered[Resource] {
          override def compare(that: Resource): Int = thisValue.stringValue().compare(that.stringValue())
        }
        var candidatePairCount = 0
        var candidatePairCountAboveThreshold = 0
        // start looking for alignment candidates
        buildSimilarityRelation(Source.fromIterator(() => normalizedTermsToAgents.iterator).mapAsync(parallelism) {
          case (term, agents) =>
            // search for agents containing term or similar
            textSearchServer.matchQuery(term, matchPercent = searchMatchPercent).map {
              hits =>
                if (hits.size >= searchSize) {
                  // when searchSize is reached, this means we miss some results..
                  logger.warn(s"[agent-attribute-identity-resolution-enricher] - Reached searchSize $searchSize for term $term.")
                }
                (term, hits, agents)
            }
        }.mapConcat {
          case (term, hits, agents) =>
            // build a list of (agent1, agent2) candidate equalities
            agents.flatMap {
              agent1 => hits.collect {
                case (agent2, _, _) if agent1 != agent2 => (agent1, agent2)
              }
            }
        }.map {
          case (agent1, agent2) =>
            // compute final equality weights
            candidatePairCount += 1
            val agent1Names = agentRepresentativeMatchNames(agent1).map(x => (x._1, x._2))
            val agent2Names = agentRepresentativeMatchNames(agent2).map(x => (x._1, x._2))
            val probability =
              if (solveDuplicateNameParts) {
                getNameTermsEqualityProbability(agent1Names, agent2Names, termIDFs, getMatchingTermIndicesWithSimilarities)
              } else {
                getNamesEqualityProbability(agent1Names, agent2Names, termIDFs, getMatchingTermSimilarities)
              }
            (agent1, agent2, probability)
        }.filter {
          case (_, _, probability) =>
            // retain candidate equalities whose probability is over a certain threshold
            if (probability >= persistenceThreshold) {
              candidatePairCountAboveThreshold += 1
              true
            } else {
              false
            }
        }).map {
          case equalities =>
            reportResults(equalities)
            logger.info(s"[agent-attribute-identity-resolution-enricher] - Efficiency: {candidatePairCount=$candidatePairCount, candidatePairCountAboveThreshold=$candidatePairCountAboveThreshold, candidatePairAboveThresholdSetSize=${equalities.size}}")
            // save equalities as owl:sameAs relations in the Repository
            repositoryConnection.begin()
            equalities.foreach {
              case ((instance1, instance2), weight) =>
                val statement = valueFactory.createStatement(instance1, OWL.SAMEAS, instance2, inferencerContext)
                if (!repositoryConnection.hasStatement(statement, false)) {
                  repositoryConnection.add(statement)
                }
              case _ =>
            }
            repositoryConnection.commit()

        }
    }
    Await.result(result, Duration.Inf)
  }

  private def inferNamePartTypes[NUMERIC: Numeric](reconciledAgentNames: IndexedSeq[(Resource, IndexedSeq[(String, (NUMERIC, IndexedSeq[IRI]))], IndexedSeq[IndexedSeq[(Seq[String], (NUMERIC, IndexedSeq[IRI]))]])],
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
                                                                reconciledAgentNames: IndexedSeq[(Resource, IndexedSeq[(String, (NUMERIC, IndexedSeq[IRI]))], IndexedSeq[IndexedSeq[(Seq[String], (NUMERIC, IndexedSeq[IRI]))]])]) = {
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
            }.sortBy(_._2._2).foldLeft((IndexedSeq.empty[NamePart], 0)) {
              case ((s, index), ((nameMatch, properties), (text, from, to))) =>
                val previous =
                  if (index != from) {
                    splitTextPart(localPart.substring(index, from))
                  } else {
                    IndexedSeq.empty
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

  private def reconcileAgentNames[NUMERIC: Numeric](agentRepresentativeNames: Map[Resource, Map[String, NUMERIC]],
                                                    agentNameParts: Map[Resource, Traversable[(String, IRI)]]) = {
    val agentAndNames = (agentRepresentativeNames.keySet ++ agentNameParts.keySet).view.map {
      case agent =>
        agent ->(agentRepresentativeNames.get(agent).map(_.toIndexedSeq).getOrElse(IndexedSeq.empty), agentNameParts.getOrElse(agent, IndexedSeq.empty))
    }.toIndexedSeq

    reconcileEntityNames(agentAndNames, getMatchingTermIndices)
  }

  private def reconcileEntityNames[ENTITY, TERM_PART, NUMERIC: Numeric](entityNames: Traversable[(ENTITY, (Traversable[(String, NUMERIC)], Traversable[(String, TERM_PART)]))],
                                                                        matchingTermIndices: (IndexedSeq[String], IndexedSeq[String]) => Seq[(Seq[Int], Seq[Int])]) = {
    val numeric = implicitly[Numeric[NUMERIC]]
    val ordering = new Ordering[(NUMERIC, IndexedSeq[TERM_PART])] {
      override def compare(x: (NUMERIC, IndexedSeq[TERM_PART]), y: (NUMERIC, IndexedSeq[TERM_PART])): Int = {
        val (xCount, xTs) = x
        val (yCount, yTs) = y
        val c = yTs.size.compareTo(xTs.size)
        if (c == 0) numeric.compare(yCount, xCount) else c
      }
    }
    def map(s: Seq[Either[NUMERIC, TERM_PART]]) = {
      s.map {
        case Left(count) => (count, IndexedSeq.empty)
        case Right(t) => (numeric.zero, Vector(t))
      }
    }
    def reduce(s: Seq[(NUMERIC, IndexedSeq[TERM_PART])]) = {
      s.foldLeft((numeric.zero, IndexedSeq.empty[TERM_PART])) {
        case ((count1, ts1), (count2, ts2)) =>
          (numeric.plus(count1, count2), ts1 ++ ts2)
      }
    }
    entityNames.map {
      case (agent, (names, nameTermTypes)) =>
        val nameCountsAndNameTermsTypes = (names.view.map {
          case (name, count) => (extractTerms(name), Left(count))
        } ++ nameTermTypes.map {
          case (name, t) => (extractTerms(name), Right(t))
        }.view).toIndexedSeq
        val reconciledNames = reconcileNames(nameCountsAndNameTermsTypes, matchingTermIndices).map {
          case (equivalentNames) =>
            equivalentNames.groupBy(_._1).map {
              case (terms, g) => terms -> reduce(map(g.map(_._2)))
            }.toIndexedSeq.sortBy(_._2)(ordering)
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
    }.toIndexedSeq
  }

  private def reconcileNames[TERM, METADATA](names: IndexedSeq[(IndexedSeq[TERM], METADATA)],
                                             matchingTermIndices: (IndexedSeq[TERM], IndexedSeq[TERM]) => Seq[(Seq[Int], Seq[Int])]) = {
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
            matchingTermIndices(name1, name2).foreach {
              case (nameTermIndexes1, nameTermIndexes2) =>
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
                      }(scala.collection.breakOut): IndexedSeq[INDEX]).foreach {
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
    nameMap.toIndexedSeq.groupBy(_._2).map {
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
    }.toIndexedSeq
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

  private def getNamesEqualityProbability[RESOURCE](names1: Traversable[(String, Double)],
                                                    names2: Traversable[(String, Double)],
                                                    termIDFs: String => Double,
                                                    termSimilarityMatch: (IndexedSeq[String], IndexedSeq[String]) => Seq[(Seq[String], Seq[String], Double)]) = {
    var weight = 0d
    var normalization = 0d
    names1.foreach {
      case (name1, name1Weight) =>
        names2.foreach {
          case (name2, name2Weight) =>
            val terms1 = extractTerms(name1)
            val terms2 = extractTerms(name2)
            if (terms1.nonEmpty && terms2.nonEmpty) {
              val similarities = termSimilarityMatch(terms1, terms2)
              val maxWeight = normalizedSoftTFIDF(termIDFs, termIDFs)(terms1, terms2, similarities)
              weight += (name1Weight * name2Weight) * maxWeight
              normalization += (name1Weight * name2Weight)
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

  /**
    *
    * @param content to extract terms from
    * @return a list of extracted terms, in their order of appearance
    */
  private def extractTerms(content: String) = {
    tokenSeparator.split(content).toIndexedSeq.filter(_.nonEmpty)
  }

  private def normalizedSoftTFIDF[T](text1TFIDF: T => Double, text2TFIDF: T => Double) = (text1: Seq[T], text2: Seq[T], similarities: Seq[(Seq[T], Seq[T], Double)]) => {
    val denominator = text1.map(text1TFIDF).sum + text2.map(text2TFIDF).sum
    if (denominator == 0d) {
      0d
    } else {
      val numerator = similarities.map {
        case (terms1, terms2, similarity) =>
          (terms1.map(text1TFIDF).sum + terms2.map(text2TFIDF).sum) * similarity
      }.sum
      Math.min(numerator / denominator, 1d)
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
              if (totalNameCountsForContact > 0 && totalNameCountsForMessages > 0) {
                (relativeWeight / totalNameCountsForContact.toDouble, (1d - relativeWeight) / totalNameCountsForMessages.toDouble)
              } else {
                (1 / s, 1 / s)
              }
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
    }.withDefaultValue(IndexedSeq.empty)
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

  /**
    *
    * @param matching a source stream of (entity1, entity2, similarity) tuples (non-symmetric)
    * @param ordered  an ordering over entities
    * @tparam ENTITY the entity's type
    * @tparam Mat    the stream's materialization value
    * @return a map assigning to each Set(entity1, entity2) the highest similarity value
    */
  private def buildSimilarityRelation[ENTITY, Mat](matching: Source[(ENTITY, ENTITY, Double), Mat])
                                                  (implicit ordered: ENTITY => Ordered[ENTITY]) = {
    val similarityRelationBuilder = new scala.collection.mutable.HashMap[(ENTITY, ENTITY), Double]
    matching.runForeach {
      case (entity1, entity2, similarity) =>
        val key = if (entity1 > entity2) {
          (entity2, entity1)
        } else {
          (entity1, entity2)
        }
        val previousSimilarity = similarityRelationBuilder.getOrElseUpdate(key, 0d)
        val newSimilarity = Math.max(previousSimilarity, similarity)
        similarityRelationBuilder += key -> newSimilarity
    }.map {
      case _ => similarityRelationBuilder.result().toMap
    }
  }

  /**
    * Computes term inverse document frequencies (IDFs) from a collection of documents
    *
    * @param documents a collection of documents, each document is given by a list of terms, and their respective appearance probability (between 0 and 1)
    * @return a TERM -> IDF map
    */
  private def computeTermIDFs(documents: Traversable[Traversable[(String, Double)]]) = {
    val n = documents.size
    val idfs = documents.flatten.groupBy(_._1).map {
      case (term, terms) =>
        term -> math.log(n / terms.map(_._2).sum)
    }
    idfs
  }

  private def getNameTermsEqualityProbability(terms1: IndexedSeq[(String, Double)],
                                              terms2: IndexedSeq[(String, Double)],
                                              termIDFs: String => Double,
                                              termSimilarityMatchIndices: (IndexedSeq[String], IndexedSeq[String]) => Seq[(Seq[Int], Seq[Int], Double)]) = {
    if (terms1.nonEmpty && terms2.nonEmpty) {
      val similarityIndices = termSimilarityMatchIndices(terms1.map(_._1), terms2.map(_._1))
      def termsTFIDFs(terms: IndexedSeq[(String, Double)]) = (index: Int) => {
        val (term, weight) = terms(index)
        weight * termIDFs(term)
      }
      normalizedSoftTFIDF(termsTFIDFs(terms1), termsTFIDFs(terms2))(terms1.indices, terms2.indices, similarityIndices)
    } else {
      0d
    }
  }

  /**
    * Report some statistics on the equality results
    *
    * @param equalities a map from a Set(agent1, agent2) to its equality probability
    */
  private def reportResults(equalities: Map[(Resource, Resource), Double]) = {
    val sortedEqualities = equalities.toIndexedSeq.sortBy(_._2)(implicitly[Ordering[Double]].reverse)
    var previous = equalities.size
    val hist = for (i <- 1 to 11) yield {
      val t = i.toDouble * 0.1
      val s = sortedEqualities.takeWhile(_._2 >= t).size
      val diff = previous - s
      previous = s
      if (t > 1d) {
        (f"x = 1", diff)
      } else {
        def format(d: Double) = {
          "%.1f".formatLocal(Locale.ROOT, d)
        }
        (s"${format(t - 0.1)} <= x < ${format(t)}", diff)
      }
    }
    logger.info(s"[agent-attribute-identity-resolution-enricher] - Result distribution: $hist")
  }

  /**
    *
    * @param  resourceNameTermTypes a list of ((resource, nameTerm), nameTermTypes)
    * @return the name part type distribution for each name term, as in
    *         [
    *         ("John",10,[(givenName,9),(familyName,1)]),
    *         ("Doe",8,[(givenName,1),(familyName,7)])
    *         ]
    */
  private def namePartTypeDistribution(resourceNameTermTypes: Traversable[((Resource, String), Traversable[IRI])]) = {
    val ordering = implicitly[Ordering[Int]].reverse
    resourceNameTermTypes.groupBy(_._1._2).map {
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
