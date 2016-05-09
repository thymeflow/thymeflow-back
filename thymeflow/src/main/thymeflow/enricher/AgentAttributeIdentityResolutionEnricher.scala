package thymeflow.enricher

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Locale

import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.apache.lucene.search.spell.LevensteinDistance
import org.openrdf.model.impl.SimpleLiteral
import org.openrdf.model.vocabulary.OWL
import org.openrdf.model.{BNode, IRI, Resource}
import org.openrdf.query.QueryLanguage
import org.openrdf.repository.RepositoryConnection
import thymeflow.actors._
import thymeflow.enricher.AgentAttributeIdentityResolutionEnricher._
import thymeflow.graph.serialization.GraphML
import thymeflow.graph.{ConnectedComponents, ShortestPath}
import thymeflow.mathematics.probability.Rand
import thymeflow.rdf.Converters._
import thymeflow.rdf.model.ModelDiff
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.text.alignment.TextAlignment
import thymeflow.text.distances.BipartiteMatchingDistance
import thymeflow.text.search.elasticsearch.FullTextSearchServer
import thymeflow.utilities.email.EmailProviderDomainList
import thymeflow.utilities.text.Normalization
import thymeflow.utilities.{IO, Memoize}

import scala.collection.immutable.NumericRange
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

/**
  * @author David Montoya
  */

/**
  *
  * Depends on @InverseFunctionalPropertyInferencer
  *
  * @param repositoryConnection   a connection to the knowledge base
  * @param solveMode              resolution algorithm
  * @param searchMatchPercent     the term percent match required for candidate equalities
  * @param searchSize             the number of candidate results for each term
  * @param parallelism            the concurrency level when looking up for candidate equalities
  * @param matchDistanceThreshold maximum distance for a term to be considered as match
  * @param contactRelativeWeight  expresses the relative weight contact card names have with respect to message names
  *                               must be equal to None or between 0 and 1
  * @param persistenceThreshold   probability threshold above which equalities are saved
  * @param debug                  debug mode
  */
class AgentAttributeIdentityResolutionEnricher(repositoryConnection: RepositoryConnection,
                                               solveMode: SolveMode = Vanilla,
                                               searchMatchPercent: Int = 70,
                                               searchSize: Int = 10000,
                                               parallelism: Int = 2,
                                               matchDistanceThreshold: Double = 1d,
                                               contactRelativeWeight: Option[Double] = Option(0.5),
                                               persistenceThreshold: Double = 0d,
                                               debug: Boolean = false) extends Enricher with StrictLogging {

  private val valueFactory = repositoryConnection.getValueFactory
  private val inferencerContext = valueFactory.createIRI(Personal.NAMESPACE, "AgentAttributeIdentityResolutionEnricher")
  private val normalizeTerm = Memoize.concurrentFifoCache(1000, uncachedNormalizeTerm _)
  /**
    * term similarity using the Levenstein distance
    */
  private val termSimilarity = {
    val levensteinDistance = new LevensteinDistance()
    (term1: String, term2: String) => {
      levensteinDistance.getDistance(normalizeTerm(term1), normalizeTerm(term2))
    }
  }

  /**
    * term sequence matching functions, used to match "Alice Wonders" with "Wondrs Alice"
    */
  private val (getMatchingTermSimilarities, getMatchingTermIndicesWithSimilarities, getMatchingTermIndices) = {
    val bipartiteMatchingDistance = new BipartiteMatchingDistance((s1, s2) => 1d - termSimilarity(s1, s2), matchDistanceThreshold)
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
    val (agentRepresentativeMap, agentEquivalenceClasses) = getSharedIdRepresentativeByAgent
    // get the list email addresses for each agent
    val agentRepresentativeEmailAddresses = getAgentEmails(agentRepresentativeMap.apply)
    // for each agent, get a count for each name
    val (agentRepresentativeContactNames, agentRepresentativeMessageNames) = getAgentNameCounts(agentRepresentativeMap.apply)
    // for each agent, get a weight for each name, taking into account the relative weight of contact card vs messages
    val agentRepresentativeNames = getAgentNameWeights(agentRepresentativeContactNames, agentRepresentativeMessageNames)
    var filteredAgentCount = 0
    // for each agent, get a weight for each name, and possibly name part types
    val agentRepresentativeMatchNames =
      (solveMode match {
        case _: DeduplicateAgentNameParts =>
          val agentNameParts = getAgentNameParts(agentRepresentativeMap.apply)
          val deduplicatedAgentNames = deduplicateAgentNameParts(agentRepresentativeNames, agentNameParts).collect {
            case (agentRepresentative, primaryNameParts, _) if primaryNameParts.nonEmpty => (agentRepresentative, primaryNameParts)
          }
          val agentRepresentativeNamesMap = deduplicatedAgentNames.map {
            case (agent, primaryNames) =>
              val agentNames = primaryNames.map {
                case (primaryName, (weight, namePartTypes)) => (primaryName, weight)
              }
              agent -> agentNames
          }.toMap
          if (solveMode == DeduplicateAgentNamePartsAndSolvePartTypes) {
            val (filteredAgentRepresentativeNamesMap, count) = filterAgentsWithNamePartTypes(agentRepresentativeNamesMap, deduplicatedAgentNames, agentRepresentativeEmailAddresses.apply)
            filteredAgentCount = count
            filteredAgentRepresentativeNamesMap
          } else {
            agentRepresentativeNamesMap
          }
        case _ =>
          agentRepresentativeNames.collect {
            case (agent, names) if names.nonEmpty => agent -> names.toIndexedSeq.map {
              case (name, weight) => (name, weight)
            }
          }
      }).withDefaultValue(IndexedSeq.empty)

    // compute a list of (agent, nameTerms)
    val agentAndNormalizedNameTerms: scala.collection.immutable.IndexedSeq[(Resource, IndexedSeq[(String, Double)])] =
      solveMode match {
        case _: DeduplicateAgentNameParts =>
          // in this case, an agent only has one name assignment through agentRepresentativeMatchNames
          agentRepresentativeMatchNames.map {
            case (agent, terms) =>
              (agent, terms.map {
                case (term, weight) => (normalizeTerm(term), weight)
              })
          }(scala.collection.breakOut)
        case _ =>
          agentRepresentativeMatchNames.map {
            case (agent, nameCounts) =>
              agent -> nameCounts.flatMap {
                case (name, weight) =>
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
    // an order on agents (comparison of their string value)
    implicit val orderedAgents = (thisValue: Resource) => new Ordered[Resource] {
      override def compare(that: Resource): Int = thisValue.stringValue().compare(that.stringValue())
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
            val agent1Names = agentRepresentativeMatchNames(agent1)
            val agent2Names = agentRepresentativeMatchNames(agent2)
            val probability =
              solveMode match {
                case _: DeduplicateAgentNameParts =>
                  getNameTermsEqualityProbability(agent1Names, agent2Names, termIDFs, getMatchingTermIndicesWithSimilarities)
                case _ =>
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
            val buckets = samplingBuckets()
            val samples = generateSamples(agentRepresentativeMap, agentEquivalenceClasses, equalities, buckets, 100)
            saveSamplesToFile(samples)
            reportStatistics(equalities, buckets)
            saveResultsToFile(equalities)
            logger.info(s"[agent-attribute-identity-resolution-enricher] - Counts: {filteredAgentCount=$filteredAgentCount, candidatePairCount=$candidatePairCount, candidatePairCountAboveThreshold=$candidatePairCountAboveThreshold, candidatePairAboveThresholdSetSize=${equalities.size}}")
            // save equalities as owl:sameAs relations in the Repository
            repositoryConnection.begin()
            equalities.foreach {
              case (agent1, agent2, _) =>
                val statement1 = valueFactory.createStatement(agent1, OWL.SAMEAS, agent2, inferencerContext)
                val statement2 = valueFactory.createStatement(agent2, OWL.SAMEAS, agent1, inferencerContext)
                repositoryConnection.add(statement1)
                repositoryConnection.add(statement2)
              case _ =>
            }
            repositoryConnection.commit()
        }
    }
    Await.result(result, Duration.Inf)
  }

  private def generateSamples(agentRepresentativeMap: Map[Resource, Resource],
                              agentEquivalenceClasses: IndexedSeq[Set[Resource]],
                              equalities: IndexedSeq[(Resource, Resource, Double)],
                              buckets: NumericRange[BigDecimal],
                              nSamples: Int) = {
    implicit val random = Random
    val agentEquivalenceClassMap = agentEquivalenceClasses.flatMap {
      agents => agents.view.map(_ -> agents)
    }.toMap.withDefault(Set(_))
    val baseMap: Map[Resource, Set[Resource]] = Map().withDefault(Set(_))
    (IndexedSeq((None: Option[BigDecimal], agentEquivalenceClassMap, agentEquivalenceClasses)).view ++ buckets.reverse.view.map {
      threshold =>
        val (map, classes) = equalityRelationEquivalentClasses(agentRepresentativeMap, equalities, threshold.toDouble)
        (Some(threshold), map, classes)
    }).foldLeft((IndexedSeq(): IndexedSeq[(Option[BigDecimal], IndexedSeq[(Resource, Resource)])], baseMap)) {
      case ((v, previousMap), (threshold, map, classes)) =>
        logger.info(s"${classes.map(_.size).sum}")
        val samples = equalitySampler(previousMap, classes).map {
          sampler =>
            val samples = (1 to nSamples).map {
              _ => sampler.draw()
            }
            logger.info(s"""${samples.map { case (agent1, agent2) => map(agent1) == map(agent2) }.forall(identity)}""")
            samples
        }.getOrElse(IndexedSeq.empty)
        (v :+(threshold, samples), map)
    }._1
  }

  private def equalitySampler(previousEquivalenceMap: Map[Resource, Set[Resource]],
                              classes: IndexedSeq[Set[Resource]])(implicit random: Random): Option[Rand[(Resource, Resource)]] = {
    val subClassSamplers = classes.flatMap {
      case clazz =>
        val subClasses = clazz.map(previousEquivalenceMap).toIndexedSeq.map {
          case subClass => (Rand.uniform(subClass.toIndexedSeq), subClass.size.toLong)
        }
        if (subClasses.size >= 2) {
          val (randomSubClassPair, count) = Rand.shuffleTwoWeightedOrdered(subClasses)
          Some((new Rand[(Resource, Resource)] {
            override def draw(): (Resource, Resource) = randomSubClassPair.draw() match {
              case (class1, class2) => (class1.draw(), class2.draw())
            }
          }, count))
        } else {
          None
        }
    }
    if (subClassSamplers.isEmpty) {
      None
    } else {
      val cumulative = Rand.cumulative(subClassSamplers)
      Some(new Rand[(Resource, Resource)] {
        override def draw(): (Resource, Resource) = {
          cumulative.draw().draw()
        }
      })
    }
  }

  private def equalityRelationEquivalentClasses(representatives: Traversable[(Resource, Resource)],
                                                equalities: Traversable[(Resource, Resource, Double)],
                                                threshold: Double) = {
    val e = equalities.view.filter(_._3 >= threshold).map(x => (x._1, x._2)) ++ representatives
    val neighbors = (e ++ e.map(x => (x._2, x._1))).groupBy(_._1).map {
      case (instance1, instances) => (instance1, instances.map(_._2).toSet)
    }
    val connectedComponents = ConnectedComponents.compute[Resource](neighbors.keys, neighbors.getOrElse(_, Traversable.empty))

    (connectedComponents.flatMap(connectedComponent => connectedComponent.view.map {
      _ -> connectedComponent
    })
      .toMap
      .withDefault(Set(_)), connectedComponents)
  }

  /**
    * Filters agents without enough name part type information.
    * It will not filter agents when name part type uncertainty is too high
    *
    * @param agentRepresentativeNamesMap the agent name part map to filter
    * @param deduplicatedAgentNames      deduplicated agent names
    * @param agentEmailAddresses         a map from an agent to its email addresses
    * @tparam NUMERIC the numeric type
    * @return
    */
  private def filterAgentsWithNamePartTypes[NUMERIC: Numeric](agentRepresentativeNamesMap: Map[Resource, IndexedSeq[(String, Double)]],
                                                              deduplicatedAgentNames: IndexedSeq[(Resource, IndexedSeq[(String, (NUMERIC, IndexedSeq[IRI]))])],
                                                              agentEmailAddresses: Resource => Traversable[String]) = {
    val namePartTypeInference = inferNamePartTypes(deduplicatedAgentNames, agentEmailAddresses)
    var filteredAgentCount = 0
    (agentRepresentativeNamesMap.filter {
      case (agent, nameParts) =>
        var notFullyInferred = false
        val namePartsTypes = nameParts.flatMap {
          case (namePart, weight) =>
            val namePartTypes = namePartTypeInference((agent, namePart)).map {
              case (partType, probability) => (partType, probability * weight)
            }.toIndexedSeq
            if (namePartTypes.isEmpty && weight > 0.25) {
              notFullyInferred = true
            }
            namePartTypes
        }.groupBy(_._1).map {
          case (partType, g) => partType -> g.map(_._2).max
        }
        if (!notFullyInferred) {
          // if agent name part has enough inferred part types, check if there is at least a family name and given name in the mix.
          // otherwise, we will not attempt to match against this agent, in an attempt to reduce false positives
          // TODO: since name parts are independent, the following weights be lower than expected
          (namePartsTypes.getOrElse(SchemaOrg.GIVEN_NAME, 0d), namePartsTypes.getOrElse(SchemaOrg.FAMILY_NAME, 0d)) match {
            case (givenNameWeight, familyNameWeight) if familyNameWeight < 0.25d || givenNameWeight < 0.25d =>
              filteredAgentCount += 1
              false
            case _ => true
          }
        } else {
          true
        }
    }, filteredAgentCount)
  }

  /**
    * Infers agent name part types
    *
    * @param deduplicatedAgentNames agent names, deduplicated
    * @param agentEmailAddresses    a map from each agent to a list of email addresses
    * @tparam NUMERIC numeric type
    * @return
    */
  private def inferNamePartTypes[NUMERIC: Numeric](deduplicatedAgentNames: IndexedSeq[(Resource, IndexedSeq[(String, (NUMERIC, IndexedSeq[IRI]))])],
                                                   agentEmailAddresses: Resource => Traversable[String]) = {
    val filterDomainList = EmailProviderDomainList.all
    val filteredAgentEmailAddresses = (agentRepresentative: Resource) => {
      agentEmailAddresses(agentRepresentative).view.map(getAgentEmailAddressParts).filter(x => !filterDomainList.contains(x._2))
    }
    val agentEmailAddressLocalPartNamePartsAlignment = matchEmailAddressLocalPartWithAgentNames(filteredAgentEmailAddresses, deduplicatedAgentNames)
    if (debug) {
      groupNamePartsByDomain(agentEmailAddressLocalPartNamePartsAlignment)
    }
    inferNamePartTypesFromGraphTransitivity(agentEmailAddressLocalPartNamePartsAlignment).apply _
  }

  /**
    * Infers agent name part types by looking up pattern links between domain email addresses and knowledge extracted from the knowledge base
    *
    * @param agentEmailAddressLocalPartNamePartsAlignment a list of agent email address local part matched patterns
    * @return
    */
  private def inferNamePartTypesFromGraphTransitivity(agentEmailAddressLocalPartNamePartsAlignment: IndexedSeq[(Resource, String, String, IndexedSeq[NamePart], Map[VariableNamePart, NamePartMatch], Double, IndexedSeq[(String, Set[IRI])])]) = {
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
        }.toIndexedSeq
        val namePartTypeDistribution =
          if (namePartTypeForNode.nonEmpty) {
            val (_, (maxDistance, _)) = namePartTypeForNode.maxBy(_._2)(pairOrdering)
            val distribution = namePartTypeForNode.filter(_._2._1 == maxDistance).map {
              case (partType, (_, weight)) => (partType, weight)
            }
            val normalizationFactor = distribution.map(_._2).sum.toDouble
            distribution.map {
              case (partType, weight) => (partType, weight.toDouble / normalizationFactor)
            }.toMap
          } else {
            Map.empty[IRI, Double]
          }
        (resource, namePart) -> namePartTypeDistribution
    }.toMap.withDefaultValue(Map.empty[IRI, Double])
    // serialize graph with results (debug only)
    if (debug) {
      serializeAgentNamePartGraph(nodes, domainToAgentNamePartEdges ++ agentNamePartToNamePartEdges, nodeToNodeIdMap.apply, agentNamePartToNamePartTypes.apply)
    }
    agentNamePartToNamePartTypes
  }

  /**
    * Writes the AgentNamePart graph to a file
    *
    * @param nodes                           the graph nodes
    * @param edges                           the graph edges
    * @param nodeId                          a map from a NamePartGraphNode to an integer id
    * @param resourceNamePartToNamePartTypes a map from a (resource, namePart) to a distribution of name part types
    */
  private def serializeAgentNamePartGraph(nodes: Set[NamePartGraphNode],
                                          edges: Set[(NamePartGraphNode, NamePartGraphNode)],
                                          nodeId: NamePartGraphNode => Int,
                                          resourceNamePartToNamePartTypes: ((Resource, String)) => Map[IRI, Double]) = {
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
        val propertiesSerialization = resourceNamePartToNamePartTypes((resource, namePart)).map {
          case (namePartType, weight) => (namePartType.getLocalName, weight)
        }.toIndexedSeq.sortBy(_._2).mkString(",")
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

    GraphML.write(Paths.get(s"data/agent-attribute-identity-resolution-enricher_name-part-types_${IO.pathTimestamp}.graphml"),
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

  /**
    * Matches patterns in email addresses local parts with names parts
    *
    * @param getAgentEmailAddresses a map from an agent to a list of email addresses
    * @param deduplicatedAgentNames the agent names
    * @tparam NUMERIC the numeric type
    * @return a list of matched patterns
    */
  private def matchEmailAddressLocalPartWithAgentNames[NUMERIC](getAgentEmailAddresses: Resource => Traversable[(String, String)],
                                                                deduplicatedAgentNames: IndexedSeq[(Resource, IndexedSeq[(String, (NUMERIC, IndexedSeq[IRI]))])]) = {
    def filter(score: Double, matches: Int, mismatches: Int) = {
      matches.toDouble / (matches + mismatches).toDouble >= 0.7
    }
    deduplicatedAgentNames.flatMap {
      case (agentRepresentative, primaryNameParts) =>
        val agentEmailAddresses = getAgentEmailAddresses(agentRepresentative)
        val normalizedNameParts = primaryNameParts.map {
          case (namePart, (_, namePartTypes)) => (normalizeTerm(namePart), namePartTypes.toSet)
        }
        agentEmailAddresses.map {
          case (localPart, domain) =>
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

  /**
    * Parses a token list out of some text by splitting at separators
    *
    * @param content the text to parse
    * @return a list of matched tokens (Left) and separators (Right)
    */
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

  private def deduplicateAgentNameParts[NUMERIC: Numeric](agentRepresentativeNames: Map[Resource, Map[String, NUMERIC]],
                                                          agentNameParts: Map[Resource, Traversable[(String, IRI)]]) = {
    val agentAndNames = (agentRepresentativeNames.keySet ++ agentNameParts.keySet).view.map {
      case agent =>
        agent ->(agentRepresentativeNames.get(agent).map(_.toIndexedSeq).getOrElse(IndexedSeq.empty), agentNameParts.getOrElse(agent, IndexedSeq.empty))
    }.toIndexedSeq

    deduplicateEntityNameParts(agentAndNames, getMatchingTermIndices)
  }

  /**
    *
    * @param entityNames the list of entities to deduplicate. Each entity comes with a name distribution and some name part types
    * @param matchingTermIndices
    * @tparam ENTITY         the entity type
    * @tparam NAME_PART_TYPE the name part type
    * @tparam NUMERIC        the numeric type
    * @return deduplicated entity name parts
    */
  private def deduplicateEntityNameParts[ENTITY, NAME_PART_TYPE, NUMERIC: Numeric](entityNames: Traversable[(ENTITY, (Traversable[(String, NUMERIC)], Traversable[(String, NAME_PART_TYPE)]))],
                                                                                   matchingTermIndices: (IndexedSeq[String], IndexedSeq[String]) => Seq[(Seq[Int], Seq[Int])]) = {
    val numeric = implicitly[Numeric[NUMERIC]]
    val ordering = new Ordering[(NUMERIC, IndexedSeq[NAME_PART_TYPE])] {
      override def compare(x: (NUMERIC, IndexedSeq[NAME_PART_TYPE]), y: (NUMERIC, IndexedSeq[NAME_PART_TYPE])): Int = {
        val (xCount, xTs) = x
        val (yCount, yTs) = y
        val c = yTs.size.compareTo(xTs.size)
        if (c == 0) numeric.compare(yCount, xCount) else c
      }
    }
    def map(s: Seq[Either[NUMERIC, NAME_PART_TYPE]]) = {
      s.map {
        case Left(count) => (count, IndexedSeq.empty)
        case Right(t) => (numeric.zero, Vector(t))
      }
    }
    def reduce(s: Seq[(NUMERIC, IndexedSeq[NAME_PART_TYPE])]) = {
      s.foldLeft((numeric.zero, IndexedSeq.empty[NAME_PART_TYPE])) {
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
        val reconciledNames = deduplicateNameParts(nameCountsAndNameTermsTypes, matchingTermIndices).map {
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

  /**
    *
    * @param names               the list of term sequences to deduplicate
    * @param matchingTermIndices a function for matching term sequences
    * @tparam TERM     the term type
    * @tparam METADATA the metadata type
    * @return deduplicated name parts
    */
  private def deduplicateNameParts[TERM, METADATA](names: IndexedSeq[(IndexedSeq[TERM], METADATA)],
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
    *
    * @param content to extract terms from
    * @return a list of extracted terms, in their order of appearance
    */
  private def extractTerms(content: String) = {
    tokenSeparator.split(content).toIndexedSeq.filter(_.nonEmpty)
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
      Math.min(weight / normalization, 1d)
    } else {
      0.0
    }
    equalityProbability
  }

  /**
    * @return a map that gives for each agent its equivalent class representative under the "shared id" (email, url...)
    *         equivalence relation, and the list of equivalence classes
    *         by default, an unknown agent is represented by itself
    */
  private def getSharedIdRepresentativeByAgent: (Map[Resource, Resource], IndexedSeq[Set[Resource]]) = {
    val sameIdAgents = getSameIdAgents
    val equivalenceClasses = ConnectedComponents.compute[Resource](sameIdAgents.keys, sameIdAgents.getOrElse(_, None))
    val agentRepresentativeMap = equivalenceClasses
      .flatMap(connectedComponent => connectedComponent.map {
        _ -> connectedComponent.collectFirst {
          case resource if !resource.isInstanceOf[BNode] => resource
        }.getOrElse(connectedComponent.head)
      })
      .toMap
      .withDefault(identity)
    (agentRepresentativeMap, equivalenceClasses)
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
      case _ => similarityRelationBuilder.view.map {
        case ((entity1, entity2), similarity) => (entity1, entity2, similarity)
      }.toIndexedSeq
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

  /**
    *
    * @param terms1                     term sequence 1
    * @param terms2                     term sequence 2
    * @param termIDFs                   term IDFs
    * @param termSimilarityMatchIndices a function for matching term sequences
    * @return the equality probability of the two term sequences
    */
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
    *
    * @param text1TFIDF tf-idf for the first text
    * @param text2TFIDF tf-idf for the second text
    * @tparam T the term type
    * @return the distance between two text sequences using cosine similarity over their TFIDF space
    */
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
    *
    * @param binSize size of the bucket
    * @return a range from 0 to 1 (inclusive) in steps of binSize
    */
  private def samplingBuckets(binSize: BigDecimal = BigDecimal("0.05")) = {
    Range.BigDecimal.inclusive(start = BigDecimal(0) + binSize, end = BigDecimal(1), step = binSize)
  }

  /**
    * @param equalities a map from a Set(agent1, agent2) to its equality probability
    * @param range      a list of threshold
    */
  private def reportStatistics(equalities: Traversable[(Resource, Resource, Double)],
                               range: NumericRange[BigDecimal]) = {
    var remainingEqualitiesSorted = equalities.toIndexedSeq.sortBy(_._3)(implicitly[Ordering[Double]])
    def format(decimal: BigDecimal) = {
      "%.2f".formatLocal(Locale.ROOT, decimal)
    }
    val buckets = for (x <- range) yield {
      val (bucket, remaining) = remainingEqualitiesSorted.partition(_._3 <= x)
      remainingEqualitiesSorted = remaining
      (x, bucket)
    }
    val hist = for ((x, bucket) <- buckets) yield {
      if (x == range.head) {
        (s"x <= ${format(x)}", bucket.size)
      } else {
        (s"${format(x - range.step)} < x <= ${format(x)}", bucket.size)
      }
    }
    logger.info(s"[agent-attribute-identity-resolution-enricher] - Result distribution: $hist")
  }

  /**
    * Save the equalities to some file
    *
    * @param equalities a map from a Set(agent1, agent2) to its equality probability
    */
  private def saveResultsToFile(equalities: Traversable[(Resource, Resource, Double)]) {
    val sortedEqualities = equalities.toIndexedSeq.sortBy(_._3)(implicitly[Ordering[Double]].reverse)
    val results = sortedEqualities.map {
      case (agent1, agent2, probability) => Vector(agent1.stringValue(), agent2.stringValue(), probability.toString).mkString(",")
    }.mkString("\n")
    val path = Paths.get(s"data/agent-attribute-identity-resolution-enricher_${IO.pathTimestamp}.csv")
    Files.write(path, results.getBytes(StandardCharsets.UTF_8))
  }


  private def saveSamplesToFile(samples: Traversable[(Option[BigDecimal], Traversable[(Resource, Resource)])]) {
    val output = samples.flatMap {
      case (threshold, samplesForThreshold) =>
        samplesForThreshold.groupBy(identity).map {
          case ((resource1, resource2), g) => (threshold.map(_.toString).getOrElse(""),
            resource1.stringValue(),
            resource2.stringValue(),
            g.size.toString).productIterator.mkString(",")
        }
    }.mkString("\n")
    val path = Paths.get(s"data/agent-attribute-identity-resolution-enricher_samples_${IO.pathTimestamp}.csv")
    Files.write(path, output.getBytes(StandardCharsets.UTF_8))
  }

  /**
    *
    * @param  resourceNamePartTypes a list of ((resource, namePart), namePartTypes)
    * @return the name part type distribution for each name term, as in
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

object AgentAttributeIdentityResolutionEnricher {

  sealed trait SolveMode

  sealed trait DeduplicateAgentNameParts extends SolveMode

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

  object Vanilla extends SolveMode

  object VanillaDeduplicateAgentNameParts extends DeduplicateAgentNameParts

  object DeduplicateAgentNamePartsAndSolvePartTypes extends DeduplicateAgentNameParts

}