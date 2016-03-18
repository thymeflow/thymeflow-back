name := "pkb"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.+"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3"

// mime4j
val mime4JLibraries = Seq(
  "org.apache.james" % "apache-mime4j-core" % "0.7.2",
  "org.apache.james" % "apache-mime4j-dom" % "0.7.2"
)
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
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.+"
libraryDependencies += "com.typesafe.akka" %% "akka-http-experimental" % "2.+" //TOOD: migrate to the stable version
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.+"
