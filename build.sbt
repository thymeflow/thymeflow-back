name := "pkb"

version := "1.0"

scalaVersion := "2.11.7"

// Logging Traits
// Logging tool
val logLibraries = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.slf4j" % "slf4j-api" % "1.7.12"
)

libraryDependencies ++= logLibraries
libraryDependencies += "org.openrdf.sesame" % "sesame-repository-sail" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-sail-memory" % "4.+" //If we want disk storage we should add sesame-sail-nativerdf
libraryDependencies += "org.openrdf.sesame" % "sesame-rio-turtle" % "4.+"
libraryDependencies += "com.googlecode.ez-vcard" % "ez-vcard" % "0.9.+"
libraryDependencies += "net.sf.biweekly" % "biweekly" % "0.4.+"
libraryDependencies += "com.googlecode.libphonenumber" % "libphonenumber" % "7.+"
libraryDependencies += "com.github.lookfirst" % "sardine" % "5.+"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.+"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.+"
libraryDependencies += "com.sun.mail" % "javax.mail" % "1.+"
libraryDependencies += "org.eclipse.jetty" % "jetty-webapp" % "9.+"