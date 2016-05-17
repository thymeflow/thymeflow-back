package thymeflow.rdf.model.vocabulary

import org.openrdf.model.IRI
import org.openrdf.model.impl.SimpleValueFactory

object Personal {
  val NAMESPACE: String = "http://thymeflow.com/personal#"
  val PREFIX: String = "personal"

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
  val DOMAIN: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "domain")
  val IN_REPLY_TO: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "inReplyTo")
  val LOCAL_PART: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "localPart")
  val MAGNITUDE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "magnitude")
  val NICKNAME: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "nickname")
  val PRIMARY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "primaryRecipient")
  val TIME: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "time")
  val UNCERTAINTY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "uncertainty")
  val VELOCITY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "velocity")
}
