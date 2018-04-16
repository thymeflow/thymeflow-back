package com.thymeflow.rdf.model.vocabulary

import org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.model.impl.SimpleValueFactory

object Personal {
  val NAMESPACE: String = "http://thymeflow.com/personal#"
  val PREFIX: String = "personal"

  val DEFAULT_GRAPH: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "defaultGraph")
  val SERVICE_GRAPH: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "serviceGraph")
  val ONTOLOGY_DEFINITION: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "ontologyGraph")

  val SERVICE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Service")
  val SERVICE_ACCOUNT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "ServiceAccount")
  val ACCOUNT_OF: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "accountOf")
  val SERVICE_ACCOUNT_SOURCE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "ServiceAccountSource")
  val SOURCE_OF: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "sourceOf")
  val DOCUMENT_OF: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "documentOf")

  val AGENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Agent")
  val CELLPHONE_NUMBER: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "CellphoneNumber")
  val CLUSTER_EVENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "ClusterEvent")
  val EMAIL_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "EmailAddress")
  val FAX_NUMBER: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "FaxNumber")
  val GEO_VECTOR: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "GeoVector")
  val HOME_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "HomeAddress")
  val LOCATION: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Location")
  val PHONE_NUMBER: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "PhoneNumber")
  val PREFERRED_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "PreferredAddress")
  val PRIMARY_FACET: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "PrimaryFacet")
  val STAY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Stay")
  val WORK_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "WorkAddress")

  val ANGLE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "angle")
  val BLIND_COPY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "blindCopyRecipient")
  val COPY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "copyRecipient")
  val DIFFERENT_FROM: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "differentFrom")
  val DOMAIN: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "domain")
  val IN_REPLY_TO: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "inReplyTo")
  val LOCAL_PART: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "localPart")
  val MAGNITUDE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "magnitude")
  val NICKNAME: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "nickname")
  val PRIMARY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "primaryRecipient")
  val SAME_AS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "sameAs")
  val PRIMARY_FACET_PROPERTY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "primaryFacet")
  val TIME: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "time")
  val UNCERTAINTY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "uncertainty")
  val VELOCITY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "velocity")

  val DURATION: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "duration")
  val DURATION_IN_MILLIS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "durationInMillis")
}
