package pkb.rdf.model.vocabulary

import org.openrdf.model.IRI
import org.openrdf.model.impl.SimpleValueFactory

/**
  * @author Thomas Pellissier Tanon
  */
object SchemaOrg {
  val NAMESPACE: String = "http://schema.org/"
  val PREFIX: String = "schema"

  val COUNTRY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Country")
  val EMAIL_MESSAGE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "EmailMessage")
  val EVENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Event")
  val GEO_COORDINATES: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "GeoCoordinates")
  val IMAGE_OBJECT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "ImageObject")
  val ORGANIZATION: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Organization")
  val PERSON: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Person")
  val PLACE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "Place")
  val POSTAL_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "PostalAddress")

  val ABOUT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "about")
  val ADDITIONAL_NAME: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "additionalName")
  val ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "address")
  val ADDRESS_COUNTRY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "addressCountry")
  val ADDRESS_LOCALITY: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "addressLocality")
  val ADDRESS_REGION: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "addressRegion")
  val ATTENDEE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "attendee")
  val AUTHOR: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "author")
  val BIRTH_DATE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "birthDate")
  val BIRTH_PLACE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "birthPlace")
  val DATE_PUBLISHED: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "datePublished")
  val DEATH_DATE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "deathDate")
  val DEATH_PLACE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "deathPlace")
  val DESCRIPTION: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "description")
  val DURATION: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "duration")
  val EMAIL: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "email")
  val END_DATE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "endDate")
  val FAMILY_NAME: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "familyName")
  val GEO: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "geo")
  val GIVEN_NAME: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "givenName")
  val HAS_PART: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "hasPart")
  val HEADLINE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "headline")
  val HONORIFIC_PREFIX: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "honorificPrefix")
  val HONORIFIC_SUFFIX: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "honorificSuffix")
  val IMAGE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "image")
  val IN_LANGUAGE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "inLanguage")
  val JOB_TITLE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "jobTitle")
  val LATITUDE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "latitude")
  val LOCATION: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "location")
  val LONGITUDE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "longitude")
  val MEMBER_OF: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "memberOf")
  val NAME: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "name")
  val ORGANIZER: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "organizer")
  val POST_OFFICE_BOX_NUMBER: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "postOfficeBoxNumber")
  val POSTAL_CODE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "postalCode")
  val START_DATE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "startDate")
  val STREET_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "streetAddress")
  val TELEPHONE: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "telephone")
  val TEXT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "text")
  val URL: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "url")
}
