import sbt.Keys._

scalaVersion := "2.11.8"

val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.11.8",
  libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.+",
  libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.+",
  libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.+",
  scalaSource in Compile := baseDirectory.value / "src/main",
  scalaSource in Test := baseDirectory.value / "src/test",
  javaSource in Compile := baseDirectory.value / "src/main",
  javaSource in Test := baseDirectory.value / "src/test"
)

val utilitiesProject = Project(
  id = "utilities",
  base = file("utilities")
).settings(commonSettings: _*).settings(
)

val mathematicsProject = Project(
  id = "mathematics",
  base = file("mathematics")
).settings(commonSettings: _*).settings(
  libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
)

val graphProject = Project(
  id = "graph",
  base = file("graph")
).settings(commonSettings: _*).settings(
)

val spatialProject = Project(
  id = "spatial",
  base = file("spatial")
).settings(commonSettings: _*).settings(
  // Very fast and accurate geodesic computations
  libraryDependencies += "net.sf.geographiclib" % "GeographicLib-Java" % "1.43"
).dependsOn(utilitiesProject, graphProject, mathematicsProject)

val coreProject = Project(
  id = "core",
  base = file("core")
).settings(commonSettings: _*).settings(
  libraryDependencies += "org.apache.james" % "apache-mime4j-core" % "0.7.+",
  libraryDependencies += "org.apache.james" % "apache-mime4j-dom" % "0.7.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-repository-sail" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-sail-memory" % "4.+", //If we want disk storage we should add sesame-sail-nativerdf
  libraryDependencies += "org.openrdf.sesame" % "sesame-rio-ntriples" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-rio-nquads" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-rio-n3" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-rio-rdfjson" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-rio-trig" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-rio-trix" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-rio-turtle" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-queryresultio-sparqljson" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-queryresultio-sparqlxml" % "4.+",
  libraryDependencies += "org.openrdf.sesame" % "sesame-queryresultio-text" % "4.+",
  //libraryDependencies += "org.openrdf.sesame" % "sesame-queryalgebra-geosparql" % "4.+", TODO: enable if we want GeoSPARQL support
  libraryDependencies += "org.openrdf.sesame" % "sesame-sail-lucene4" % "4.+",
  libraryDependencies += "com.googlecode.ez-vcard" % "ez-vcard" % "0.9.+",
  libraryDependencies += "net.sf.biweekly" % "biweekly" % "0.4.+",
  libraryDependencies += "com.googlecode.libphonenumber" % "libphonenumber" % "7.+",
  libraryDependencies += "com.github.lookfirst" % "sardine" % "5.+",
  libraryDependencies += "com.sun.mail" % "javax.mail" % "1.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-http-experimental" % "2.+", //TOOD: migrate to the stable version
  libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.+",
  libraryDependencies += "org.apache.lucene" % "lucene-suggest" % "4.+"
).dependsOn(utilitiesProject)

val thymeflowProject = Project (
  id="thymeflow",
  base=file("thymeflow")
).settings(commonSettings:_*).settings(
  libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.3.+",
  libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.+"
).dependsOn(coreProject, graphProject, spatialProject)
