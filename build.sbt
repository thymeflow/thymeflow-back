name := "pkb"

lazy val rootSettings = Seq(
  version := "0.1",
  scalaVersion := "2.11.8",
  libraryDependencies ++= testLibraries
)
lazy val commonSettings = rootSettings ++ Seq(
  scalaSource in Compile := baseDirectory.value / "src/main",
  scalaSource in Test := baseDirectory.value / "src/test",
  javaSource in Compile := baseDirectory.value / "src/main",
  javaSource in Test := baseDirectory.value / "src/test"
)
lazy val pkb = (project in file(".")).settings(rootSettings)

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.+"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3"
lazy val thymeflowProject = Project (
  id="thymeflow",
  base=file("thymeflow")
).settings(commonSettings:_*).settings(
  libraryDependencies += "org.elasticsearch" % "elasticsearch" % "2.2.0",
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

libraryDependencies ++= mime4JLibraries
libraryDependencies += "org.openrdf.sesame" % "sesame-repository-sail" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-sail-memory" % "4.+" //If we want disk storage we should add sesame-sail-nativerdf
libraryDependencies += "org.openrdf.sesame" % "sesame-rio-ntriples" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-rio-nquads" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-rio-n3" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-rio-rdfjson" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-rio-trig" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-rio-trix" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-rio-turtle" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-queryresultio-sparqljson" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-queryresultio-sparqlxml" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-queryresultio-text" % "4.+"
libraryDependencies += "com.googlecode.ez-vcard" % "ez-vcard" % "0.9.+"
libraryDependencies += "net.sf.biweekly" % "biweekly" % "0.4.+"
libraryDependencies += "com.googlecode.libphonenumber" % "libphonenumber" % "7.+"
libraryDependencies += "com.github.lookfirst" % "sardine" % "5.+"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.+" //Lf2SpacesIndenter is no more available on Jackson 2.7
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.+"
libraryDependencies += "com.sun.mail" % "javax.mail" % "1.+"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.+"
libraryDependencies += "io.spray" %% "spray-can" % "1.+"
libraryDependencies += "io.spray" %% "spray-routing" % "1.+"
val testLibraries = Seq("org.scalatest" %% "scalatest" % "2.2.4")
// mime4j
val mime4JLibraries = Seq(
  "org.apache.james" % "apache-mime4j-core" % "0.7.2",
  "org.apache.james" % "apache-mime4j-dom" % "0.7.2"
)
