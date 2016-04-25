package pkb.rdf.model.vocabulary

import org.openrdf.model.IRI
import org.openrdf.model.impl.SimpleValueFactory

object Personal {
  val NAMESPACE: String = "http://thomas.pellissier-tanon.fr/personal#"
  val PREFIX: String = "personal"

  val AGENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Agent")
  val CELLPHONE_NUMBER: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "CellphoneNumber")
  val FAX_NUMBER: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "FaxNumber")
  val EMAIL_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "EmailAddress")
  val HOME_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "HomeAddress")
  val PHONE_NUMBER: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "PhoneNumber")
  val PREFERRED_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "PreferredAddress")
  val PRIMARY_FACET: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "PrimaryFacet")
  val WORK_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "WorkAddress")

  val BLIND_COPY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "blindCopyRecipient")
  val COPY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "copyRecipient")
  val DOMAIN: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "domain")
  val LOCAL_PART: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "localPart")
  val NICKNAME: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "nickname")
  val PRIMARY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "primaryRecipient")
  val IN_REPLY_TO: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "inReplyTo")

  // Geo
  val TIME_GEO_LOCATION: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "TimeGeoLocation")
  val GEO_VECTOR: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "GeoVector")
  val UNCERTAINTY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "uncertainty")
  val VELOCITY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "velocity")
  val MAGNITUDE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "magnitude")
  val ANGLE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "angle")

  val STAY_EVENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "StayEvent")
  val CLUSTER_EVENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "ClusterEvent")
}
