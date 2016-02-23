name := "pkb"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.+"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.+"
libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-repository-sail" % "4.+"
libraryDependencies += "org.openrdf.sesame" % "sesame-sail-memory" % "4.+" //If we want disk storage we should add sesame-sail-nativerdf
libraryDependencies += "org.openrdf.sesame" % "sesame-rio-turtle" % "4.+"
libraryDependencies += "org.mnode.ical4j" % "ical4j" % "1.+"
libraryDependencies += "com.googlecode.ez-vcard" % "ez-vcard" % "0.9.9"
libraryDependencies += "com.googlecode.libphonenumber" % "libphonenumber" % "7.+"
libraryDependencies += "com.github.lookfirst" % "sardine" % "5.+"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.+"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.+"
