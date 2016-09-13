package com.thymeflow.enricher.entityresolution

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.function.Consumer

import com.opencsv.CSVReader
import com.thymeflow.mathematics.probability.{DiscreteRand, Rand}
import org.openrdf.model.Resource

import scala.collection.JavaConverters._
import scala.collection.SeqView
import scala.util.Random

/**
  * @author David Montoya
  */
trait EntityResolutionEvaluation extends EntityResolution {

  protected def outputFilePrefix: String

  protected def parseSamplesFromFile(path: Path,
                                     uriStringToResource: String => Resource) = {
    val reader = new CSVReader(new BufferedReader(new InputStreamReader(Files.newInputStream(path), "UTF-8"), 4096))
    val builder = IndexedSeq.newBuilder[(Option[BigDecimal], Resource, Resource)]
    reader.forEach(new Consumer[Array[String]] {
      override def accept(t: Array[String]): Unit = {
        val rawThreshold = t(0)
        val threshold = if (rawThreshold.isEmpty) {
          None
        } else {
          Some(BigDecimal(rawThreshold))
        }
        val agent1 = uriStringToResource(t(1))
        val agent2 = uriStringToResource(t(2))
        builder += ((threshold, agent1, agent2))
      }
    })
    reader.close()
    builder.result()
  }

  protected def saveEvaluationToFile(fileSuffix: String,
                                     evaluatedSamples: SeqView[(Option[BigDecimal], Option[BigDecimal], Resource, Resource, Boolean), Seq[_]]) = {
    val results = evaluatedSamples.map {
      case (threshold, sampleThreshold, resource1, resource2, truth) =>
        Vector(threshold.map(_.toString).getOrElse(""),
          sampleThreshold.map(_.toString).getOrElse(""),
          resource1.stringValue(),
          resource2.stringValue(),
          truth.toString).mkString(",")
    }
    val path = Paths.get(s"${outputFilePrefix}_evaluation_$fileSuffix.csv")
    Files.write(path, results.asJava, StandardCharsets.UTF_8)
  }

  protected def evaluateSamples(samples: IndexedSeq[(Option[BigDecimal], Resource, Resource)],
                                equivalentClasses: Seq[(Option[BigDecimal], Map[Resource, Set[Resource]], IndexedSeq[Set[Resource]])]) = {
    equivalentClasses.view.flatMap {
      case (threshold, map, classes) =>
        samples.map {
          case (sampleThreshold, resource1, resource2) =>
            (threshold, sampleThreshold, resource1, resource2, map(resource1).head == map(resource2).head)
        }
    }
  }

  protected def generateSamples(equivalentClasses: Seq[(Option[BigDecimal], Map[Resource, Set[Resource]], IndexedSeq[Set[Resource]])],
                                nSamples: Int) = {
    implicit val random = Random
    val baseMap: Map[Resource, Set[Resource]] = Map().withDefault(Set(_))
    equivalentClasses.foldLeft((IndexedSeq(): IndexedSeq[(Option[BigDecimal], Long, IndexedSeq[(Resource, Resource)])], baseMap)) {
      case ((v, previousMap), (threshold, map, classes)) =>
        val (samples, sampleGroupSize) = equalitySampler(previousMap, classes).map {
          case (sampler, samplerSize) =>
            val samples = if (nSamples >= samplerSize) {
              val s = sampler.all().toIndexedSeq
              assert(s.size == samplerSize)
              s
            } else {
              (1 to nSamples).map {
                _ => sampler.draw()
              }
            }
            assert(samples.map { case (agent1, agent2) => map(agent1) == map(agent2) }.forall(identity))
            (samples, samplerSize)
        }.getOrElse((IndexedSeq.empty, 0L))
        (v :+(threshold, sampleGroupSize, samples), map)
    }._1
  }

  private def equalitySampler(previousEquivalenceMap: Map[Resource, Set[Resource]],
                              classes: IndexedSeq[Set[Resource]])(implicit random: Random): Option[(DiscreteRand[(Resource, Resource)], Long)] = {
    val subClassSamplers = classes.flatMap {
      case clazz =>
        val subClasses = clazz.map(previousEquivalenceMap).toIndexedSeq.map {
          case subClass => (Rand.uniform(subClass.toIndexedSeq), subClass.size.toLong)
        }
        if (subClasses.size >= 2) {
          val (randomSubClassPair, count) = Rand.shuffleTwoWeightedOrdered(subClasses)
          Some((new DiscreteRand[(Resource, Resource)] {
            override def draw(): (Resource, Resource) = randomSubClassPair.draw() match {
              case (class1, class2) => (class1.draw(), class2.draw())
            }

            override def all(): Traversable[(Resource, Resource)] = {
              randomSubClassPair.all().flatMap {
                case (e1Sampler, e2Sampler) =>
                  e1Sampler.all().flatMap {
                    e1 => e2Sampler.all().map {
                      e2 => (e1, e2)
                    }
                  }
              }
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
      Some((new DiscreteRand[(Resource, Resource)] {
        override def draw(): (Resource, Resource) = {
          cumulative.draw().draw()
        }

        def all(): Traversable[(Resource, Resource)] = {
          cumulative.all().flatMap {
            _.all()
          }
        }
      }, cumulative.size))
    }
  }

  /**
    *
    * @param binSize size of the bucket
    * @return a range from 0 to 1 (inclusive) in steps of binSize
    */
  protected def samplingBuckets(binSize: BigDecimal = BigDecimal("0.025"), start: BigDecimal = BigDecimal(0)) = {
    Range.BigDecimal.inclusive(start = start + binSize, end = BigDecimal(1), step = binSize)
  }

  protected def saveSamplesToFile(fileSuffix: String,
                                  samples: Traversable[(Option[BigDecimal], Long, Traversable[(Resource, Resource)])]) {
    val sampleEqualitiesOutput = samples.flatMap {
      case (threshold, size, samplesForThreshold) =>
        samplesForThreshold.groupBy(identity).map {
          case ((resource1, resource2), g) => (threshold.map(_.toString).getOrElse("Infinity"),
            resource1.stringValue(),
            resource2.stringValue(),
            g.size.toString).productIterator.mkString(",")
        }
    }.mkString("\n")
    val sampleEqualitiesPath = Paths.get(s"${outputFilePrefix}_samples_$fileSuffix.csv")
    Files.write(sampleEqualitiesPath, sampleEqualitiesOutput.getBytes(StandardCharsets.UTF_8))
    val sampleGroupSizes = samples.map {
      case (threshold, size, _) => (threshold.map(_.toString).getOrElse("Infinity"), size.toString).productIterator.mkString(",")
    }.mkString("\n")
    val sampleGroupSizesPath = Paths.get(s"${outputFilePrefix}_samples-group-sizes_$fileSuffix.csv")
    Files.write(sampleGroupSizesPath, sampleGroupSizes.getBytes(StandardCharsets.UTF_8))
  }

  protected def saveFunctionalities(fileSuffix: String,
                                    equivalentClasses: Seq[(Option[BigDecimal], IndexedSeq[Set[Resource]])],
                                    agentRepresentativeMatchNames: Map[Resource, IndexedSeq[(String, Double)]],
                                    emailAddressForAgent: Resource => Traversable[String]) = {
    val functionalities = equivalentClasses.view.map {
      case (threshold, classes) =>
        val emailRelation = classes.zipWithIndex.map {
          case (clazz, index) => index -> clazz.flatMap(emailAddressForAgent.apply)
        }
        val (emailFunc, emailInvFunc) = (computeFunctionality(emailRelation), computeInverseFunctionality(emailRelation))
        val nameRelation = classes.zipWithIndex.map {
          case (clazz, index) => index -> clazz.flatMap(x => agentRepresentativeMatchNames.apply(x).map(y => normalizeTerm(y._1)))
        }
        val (nameFunc, nameInvFunc) = (computeFunctionality(nameRelation), computeInverseFunctionality(nameRelation))
        IndexedSeq(threshold.getOrElse(""), emailFunc, emailInvFunc, nameFunc, nameInvFunc).map(_.toString).mkString(",")
    }
    val functionalitiesPath = Paths.get(s"${outputFilePrefix}_functionalities_$fileSuffix.csv")
    Files.write(functionalitiesPath, functionalities.asJava, StandardCharsets.UTF_8)
  }

  protected def computeInverseFunctionality[X, Y](instances: Traversable[(X, Traversable[Y])]) = {
    computeFunctionality(instances.view.flatMap {
      case (x, yS) => yS.map {
        y => (y, x)
      }
    }.groupBy(_._1).map {
      case (y, g) => y -> g.map(_._2).toSet.toIndexedSeq
    })
  }

  protected def computeFunctionality[X, Y](instances: Traversable[(X, Traversable[Y])]) = {
    instances.count(_._2.nonEmpty).toDouble / Math.max(instances.map(_._2.size).sum, 1).toDouble
  }

  protected def saveClassSizes(fileSuffix: String,
                               equivalentClasses: Seq[(Option[BigDecimal], IndexedSeq[Set[Resource]])],
                               emailAddressForAgent: Resource => Traversable[String]) = {
    def save(classSizes: IndexedSeq[(Option[BigDecimal], Int, Int)], suffix: String = "") {
      val results = classSizes.view.map {
        case (threshold, classSize, classCount) =>
          Vector(threshold.map(_.toString).getOrElse(""), classSize, classCount).mkString(",")
      }
      val path = Paths.get(s"${outputFilePrefix}_class-sizes${suffix}_$fileSuffix.csv")
      Files.write(path, results.asJava, StandardCharsets.UTF_8)
    }
    var classSizesBuilder = IndexedSeq.newBuilder[(Option[BigDecimal], Int, Int)]
    var uniqueClassSizesBuilder = IndexedSeq.newBuilder[(Option[BigDecimal], Int, Int)]
    equivalentClasses.foreach {
      case (threshold, classes) =>
        classSizesBuilder ++= buildClassSizes(threshold, classes)
        uniqueClassSizesBuilder ++= buildUniqueEmailClassSizes(fileSuffix, threshold, classes, emailAddressForAgent)
    }
    save(classSizesBuilder.result())
    save(uniqueClassSizesBuilder.result(), "_unique-emails")
  }

  protected def buildUniqueEmailClassSizes(fileSuffix: String,
                                           threshold: Option[BigDecimal],
                                           classes: IndexedSeq[Set[Resource]],
                                           emailAddress: Resource => Traversable[String]) = {
    val emailClasses = classes.map {
      clazz => clazz.flatMap(emailAddress)
    }
    val distinctEmails = emailClasses.flatten.distinct.sorted
    val path = Paths.get(s"${outputFilePrefix}_distinct-emails_${threshold}_$fileSuffix.csv")
    Files.write(path, distinctEmails.asJava, StandardCharsets.UTF_8)
    classes.map {
      clazz =>
        val clazzEmailAddresses = clazz.toIndexedSeq.map(emailAddress)
        clazzEmailAddresses.flatten.distinct.size
    }.groupBy(identity).map {
      case (classSize, classesForClassSize) =>
        (threshold, classSize, classesForClassSize.size)
    }.toIndexedSeq.sortBy(_._2)
  }

  protected def buildClassSizes(threshold: Option[BigDecimal], classes: IndexedSeq[Set[Resource]]) = {
    classes.groupBy(_.size).map {
      case (classSize, classesForClassSize) =>
        (threshold, classSize, classesForClassSize.size)
    }.toIndexedSeq.sortBy(_._2)
  }
}
