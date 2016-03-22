import sbt.Keys._

name := "pkb"

val testLibraries = Seq("org.scalatest" %% "scalatest" % "2.2.4")
val logLibraries = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.+",
  "ch.qos.logback" % "logback-classic" % "1.1.3"
)
// mime4j
val mime4JLibraries = Seq(
  "org.apache.james" % "apache-mime4j-core" % "0.7.2",
  "org.apache.james" % "apache-mime4j-dom" % "0.7.2"
)

val rootSettings = Seq(
  version := "0.1",
  scalaVersion := "2.11.8",
  libraryDependencies ++= testLibraries
)
val commonSettings = rootSettings ++ Seq(
  scalaSource in Compile := baseDirectory.value / "src/main",
  scalaSource in Test := baseDirectory.value / "src/test",
  javaSource in Compile := baseDirectory.value / "src/main",
  javaSource in Test := baseDirectory.value / "src/test"
)

val pkb = (project in file(".")).settings(rootSettings:_*).settings(
  libraryDependencies ++= logLibraries,
  libraryDependencies ++= mime4JLibraries,
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
  libraryDependencies += "com.googlecode.ez-vcard" % "ez-vcard" % "0.9.+",
  libraryDependencies += "net.sf.biweekly" % "biweekly" % "0.4.+",
  libraryDependencies += "com.googlecode.libphonenumber" % "libphonenumber" % "7.+",
  libraryDependencies += "com.github.lookfirst" % "sardine" % "5.+",
  libraryDependencies += "com.sun.mail" % "javax.mail" % "1.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.+",
  libraryDependencies += "com.typesafe.akka" %% "akka-http-experimental" % "2.+", //TOOD: migrate to the stable version
  libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.+",
  libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.5"
)

val thymeflowProject = Project (
  id="thymeflow",
  base=file("thymeflow")
).settings(commonSettings:_*).settings(
  libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.3.+",
  // Breeze is a library for numerical processing
  libraryDependencies ++= Seq(
    "org.scalanlp" %% "breeze" % "0.12"
    // native libraries are not included by default. add this if you want them (as of 0.7)
    // native libraries greatly improve performance, but increase jar sizes.
    // It also packages various blas implementations, which have licenses that may or may not
    // be compatible with the Apache License. No GPL code, as best I know.
    // NOTE: This has to be specifically tested if included
    // , "org.scalanlp" %% "breeze-natives" % "0.12"
  )
).dependsOn(pkb)


