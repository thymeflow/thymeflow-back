import sbt.Keys._

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

scalacOptions += "-target:jvm-1.8"

val rootSettings = Seq(
  version := "0.1",
  scalaVersion := "2.11.8",
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions += "-target:jvm-1.8",
  libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.+" % "test"
    exclude("org.scala-lang", "scala-reflect")
    exclude("org.scala-lang.modules", "scala-xml_2.11"),
  libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.+",
  libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.+"
)

val commonSettings = rootSettings ++ Seq(
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
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-repository-sail" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-sail-memory" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-sail-nativerdf" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-ntriples" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-nquads" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-n3" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-rdfjson" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-rdfxml" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-trig" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-trix" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-queryresultio-sparqljson" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-queryresultio-sparqlxml" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-queryresultio-text" % "2.1.+",
  libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-queryalgebra-geosparql" % "2.1.+",
  //libraryDependencies += "org.eclipse.rdf4j" % "rdf4j-sail-lucene" % "2.1.+",
  libraryDependencies += "commons-logging" % "commons-logging" % "1.+",
  libraryDependencies += "com.googlecode.ez-vcard" % "ez-vcard" % "0.9.+",
  libraryDependencies += "net.sf.biweekly" % "biweekly" % "0.4.+",
  libraryDependencies += "com.googlecode.libphonenumber" % "libphonenumber" % "7.+",
  libraryDependencies += "com.github.lookfirst" % "sardine" % "5.+",
  libraryDependencies += "com.sun.mail" % "javax.mail" % "1.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.4.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.0.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.+"
).dependsOn(utilitiesProject)

// TODO: Consider making thymeflowProject the root one.
val thymeflowProject = Project (
  id="thymeflow",
  base=file("thymeflow")
).settings(commonSettings:_*).settings(
  mainClass in Compile := Some("com.thymeflow.api.MainApi"),
  libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.+"
).dependsOn(coreProject, graphProject, spatialProject)

// rootProject configuration is required to avoid downloading multiple Scala versions
// rootProject aggregates all other projects, which is convenient for running global SBT tasks (e.g. test)
val rootProject = Project(
  id = "thymeflow-back",
  base = file(".")
).settings(rootSettings: _*).aggregate(
  utilitiesProject, mathematicsProject, graphProject, spatialProject, coreProject, thymeflowProject
)
