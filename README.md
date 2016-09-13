# thymeflow-back #

`thymeflow-back` is the backend implementation of Thymeflow, a Web application that provides that:

 - Loads mail (IMAP/RFC2822), calendar (CalDav/iCalendar), contact (CardDav/vCard), location data (Google Location History) into an RDF store.
 - Provides easy to use configurators for Google/Microsoft accounts using OAuth.
 - Extracts Stays/Moves from Location data.
 - Automatically infers alignments between contact and mail data (persons), calendar and location data (event locations).
 - Provides a SPARQL endpoint for querying the RDF store, which features full-text search (Lucene SAIL).
 
For Knowledge representation, Thymeflow uses the http://schema.org ontology where possible, and its custom http://thymeflow.com/personal ontology otherwise.
 
`thymeflow-front` is the frontend implementation, which provides an interface for configuring Thymeflow, and data visualization.

`thymeflow-back` was principally built using the following technologies:

 - [SBT](http://www.scala-sbt.org/) For building the project.
 - [Sesame (now known as RDF4J)](http://rdf4j.org/doc/) for the RDF Store.
 - [Akka Actor System and Streams](http://akka.io/docs/) for the HTTP Server and Clients, and the scheduling of loading and enriching stages.

## Configuration ##

 - Refer to `core/src/main/resources/reference.conf` for a list of configurable properties.
 - Google/Microsoft/Facebook OAuth client id/secret and Google geocoder API key should be configured.
 - Customize them by creating `thymeflow/src/main/resources/application.conf` and overriding the listed properties.

## Usage ##

 - Install SBT 0.13 (http://www.scala-sbt.org/download.html).
 - Within the project's directory, run `sbt` in a console.
 - Within the SBT console, execute `thymeflow/run [userGraphFile]` where userGraphFile is an optional path to a file for storing the user local Graph.
 - By default, `thymeflow-back` will bind to `localhost:8080`. The SPARQL endpoint is located at `/sparql`.
 - Use `thymeflow/front` for the Web interface.
 
## License/Copyright 

 - Copyright is described in `COPYRIGHT.txt`.
 - License: AGPL v3, as described in `LICENSE.txt`.