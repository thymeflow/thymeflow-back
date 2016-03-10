package pkb.sync.converter

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.scalalogging.StrictLogging
import ezvcard.parameter.{AddressType, EmailType, TelephoneType}
import ezvcard.property._
import ezvcard.util.DataUri
import ezvcard.{Ezvcard, VCard}
import org.openrdf.model._
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import pkb.rdf.model.vocabulary.{Personal, SchemaOrg}
import pkb.sync.converter.utils._

import scala.collection.JavaConverters._

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class VCardConverter(valueFactory: ValueFactory) extends Converter with StrictLogging {

  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  private val phoneNumberConverter = new PhoneNumberConverter(valueFactory, "FR")
  //TODO: guess?
  private val uuidConverter = new UUIDConverter(valueFactory)

  def convert(stream: InputStream): Model = {
    convert(Ezvcard.parse(stream).all.asScala)
  }

  def convert(str: String): Model = {
    convert(Ezvcard.parse(str).all.asScala)
  }

  private def convert(vCards: Traversable[VCard]): Model = {
    val model = new LinkedHashModel
    vCards.foreach(vCard =>
      convert(vCard, model)
    )
    model
  }

  private def convert(vCard: VCard, model: Model): Resource = {
    val cardResource = resourceFromUid(vCard.getUid)
    model.add(cardResource, RDF.TYPE, Personal.AGENT)

    vCard.getProperties.asScala.foreach {
      //ADR
      case address: Address => model.add(cardResource, SchemaOrg.ADDRESS, convert(address, model))
      //BDAY
      case birthday: Birthday => convert(birthday).foreach(date => model.add(cardResource, SchemaOrg.BIRTH_DATE, date))
      //NICKNAME
      case nicknames: Nickname =>
        nicknames.getValues.asScala.foreach(nickname =>
          model.add(cardResource, Personal.NICKNAME, valueFactory.createLiteral(nickname))
        )
      //DEATHDATE
      case deathDay: Deathdate => convert(deathDay).foreach(date => model.add(cardResource, SchemaOrg.DEATH_DATE, date))
      //EMAIL
      case email: Email => model.add(cardResource, SchemaOrg.EMAIL, convert(email, model))
      //FN
      case formattedName: FormattedName => model.add(cardResource, SchemaOrg.NAME, valueFactory.createLiteral(formattedName.getValue))
      //N
      case structuredName: StructuredName =>
        Option(structuredName.getFamily).foreach(familyName =>
          model.add(cardResource, SchemaOrg.FAMILY_NAME, valueFactory.createLiteral(familyName))
        )
        Option(structuredName.getGiven).foreach(givenName =>
          model.add(cardResource, SchemaOrg.GIVEN_NAME, valueFactory.createLiteral(givenName))
        )
        structuredName.getAdditional.asScala.foreach(additionalName =>
          model.add(cardResource, SchemaOrg.ADDITIONAL_NAME, valueFactory.createLiteral(additionalName))
        )
        structuredName.getPrefixes.asScala.foreach(prefix =>
          model.add(cardResource, SchemaOrg.HONORIFIC_PREFIX, valueFactory.createLiteral(prefix))
        )
        structuredName.getPrefixes.asScala.foreach(suffix =>
          model.add(cardResource, SchemaOrg.HONORIFIC_SUFFIX, valueFactory.createLiteral(suffix))
        )
      //ORG
      case organization: Organization => model.add(cardResource, SchemaOrg.MEMBER_OF, convert(organization, model))
      //PHOTO
      case photo: Photo =>
        convert(photo, model).foreach(photoResource =>
          model.add(cardResource, SchemaOrg.IMAGE, photoResource)
        )
      //TEL
      case telephone: Telephone =>
        convert(telephone, model).foreach(telephoneResource =>
          model.add(cardResource, SchemaOrg.TELEPHONE, telephoneResource)
        )
      //TITLE
      case title: Title => model.add(cardResource, SchemaOrg.JOB_TITLE, valueFactory.createLiteral(title.getValue))
      //URL
      case url: Url => convert(url).foreach(url => model.add(cardResource, SchemaOrg.URL, url)) //TODO: Google: support link to other accounts encoded as URLs like http\://www.google.com/profiles/112359482310702047642
      //X-SOCIALPROFILE
      case property: RawProperty if property.getPropertyName == "X-SOCIALPROFILE" =>
        resourceFromUrl(property.getValue).foreach(url => model.add(cardResource, SchemaOrg.URL, url)) //TODO: better relation than schema:url
      case property => logger.info("Unsupported parameter type:" + property)
    }

    cardResource
  }

  private def convert(address: Address, model: Model): Resource = {
    val addressResource = valueFactory.createBNode
    model.add(addressResource, RDF.TYPE, SchemaOrg.POSTAL_ADDRESS)
    //TODO: getExtendedAddresses
    address.getStreetAddresses.asScala.foreach(street =>
      model.add(addressResource, SchemaOrg.STREET_ADDRESS, valueFactory.createLiteral(street))
    )
    address.getLocalities.asScala.foreach(locality =>
      model.add(addressResource, SchemaOrg.ADDRESS_LOCALITY, convertToResource(locality, model, SchemaOrg.PLACE))
    )
    address.getRegions.asScala.foreach(region =>
      model.add(addressResource, SchemaOrg.ADDRESS_REGION, convertToResource(region, model, SchemaOrg.PLACE))
    )
    address.getCountries.asScala.foreach(country =>
      model.add(addressResource, SchemaOrg.ADDRESS_COUNTRY, convertToResource(country, model, SchemaOrg.COUNTRY, SchemaOrg.PLACE))
    )
    address.getPoBoxes.asScala.foreach(poBox =>
      model.add(addressResource, SchemaOrg.POST_OFFICE_BOX_NUMBER, valueFactory.createLiteral(poBox))
    )
    address.getPostalCodes.asScala.foreach(postalCode =>
      model.add(addressResource, SchemaOrg.POSTAL_CODE, valueFactory.createLiteral(postalCode))
    )
    address.getTypes.asScala.foreach(addressType =>
      model.add(addressResource, RDF.TYPE, classForAddressType(addressType))
    )
    addressResource
  }

  private def classForAddressType(addressType: AddressType): IRI = {
    addressType match {
      case AddressType.HOME => Personal.HOME_ADDRESS
      case AddressType.PREF => Personal.PREFERRED_ADDRESS
      case AddressType.WORK => Personal.WORK_ADDRESS
      case _ => SchemaOrg.POSTAL_ADDRESS
    }
  }

  private def convertToResource(str: String, model: Model, rdfTypes: IRI*): Resource = {
    val placeResource = valueFactory.createBNode()
    rdfTypes.foreach(rdfType => model.add(placeResource, RDF.TYPE, rdfType))
    model.add(placeResource, SchemaOrg.NAME, valueFactory.createLiteral(str))
    placeResource
  }

  private def convert(dateTime: DateOrTimeProperty): Option[Literal] = {
    Option(dateTime.getDate).map(date => convert(date, dateTime.hasTime))
  }

  private def convert(date: Date, hasTime: Boolean): Literal = {
    if (date.getYear == 70) {
      //Google use 1970 as year if there is no year set
      valueFactory.createLiteral(new SimpleDateFormat("MM-dd").format(date), XMLSchema.GMONTHDAY)
    } else if (hasTime) {
      valueFactory.createLiteral(date)
    } else {
      valueFactory.createLiteral(new SimpleDateFormat("yyyy-MM-dd").format(date), XMLSchema.DATE)
    }
  }

  private def convert(email: Email, model: Model): Resource = {
    val emailResource = emailAddressConverter.convert(email.getValue, model)
    email.getTypes.asScala.foreach(emailType =>
      model.add(emailResource, RDF.TYPE, classForEmailType(emailType))
    )
    emailResource
  }

  private def classForEmailType(emailType: EmailType): IRI = {
    emailType match {
      case EmailType.HOME => Personal.HOME_ADDRESS
      case EmailType.PREF => Personal.PREFERRED_ADDRESS
      case EmailType.WORK => Personal.WORK_ADDRESS
      case _ => Personal.EMAIL_ADDRESS
    }
  }

  private def convert(photo: ImageProperty, model: Model): Option[Resource] = {
    Option(photo.getUrl).map(uri => {
      valueFactory.createIRI(uri)
    }).orElse({
      Option(photo.getData).flatMap(binary => {
        Option(photo.getContentType).map(contentType => {
          valueFactory.createIRI(new DataUri(contentType.getMediaType, binary).toString)
        }).orElse({
          logger.warn("Photo without content type")
          None
        })
      })
    }).map(photoResource => {
      model.add(photoResource, RDF.TYPE, SchemaOrg.IMAGE_OBJECT)
      photoResource
    })
  }

  private def convert(organization: Organization, model: Model): Resource = {
    convertToResource(organization.getValues.get(0), model, SchemaOrg.ORGANIZATION) //TODO: support hierarchy?
  }

  private def convert(telephone: Telephone, model: Model): Option[Resource] = {
    Option(telephone.getUri).flatMap(uri => {
      phoneNumberConverter.convert(uri.toString, model)
    }).orElse({
      Option(telephone.getText).flatMap(rawNumber => {
        phoneNumberConverter.convert(rawNumber, model)
      })
    }).map(telephoneResource => {
      telephone.getTypes.asScala.foreach(telephoneType =>
        model.add(telephoneResource, RDF.TYPE, classForTelephoneType(telephoneType))
      )
      telephoneResource
    })
  }

  private def classForTelephoneType(telephoneType: TelephoneType): IRI = {
    telephoneType match {
      case TelephoneType.CELL => Personal.CELLPHONE_NUMBER
      case TelephoneType.FAX => Personal.FAX_NUMBER
      case TelephoneType.HOME => Personal.HOME_ADDRESS
      case TelephoneType.PREF => Personal.PREFERRED_ADDRESS
      case TelephoneType.WORK => Personal.WORK_ADDRESS
      case _ => Personal.PHONE_NUMBER
      }
  }

  private def convert(url: UriProperty): Option[Resource] = {
    resourceFromUrl(url.getValue)
  }

  private def resourceFromUid(uid: Uid): Resource = {
    if (uid == null) {
      valueFactory.createBNode()
    } else {
      uuidConverter.convert(uid.getValue)
    }
  }

  private def resourceFromUrl(url: String): Option[Resource] = {
    try {
      Some(valueFactory.createIRI(url))
    } catch {
      case e: IllegalArgumentException =>
        logger.warn("The URL " + url + " is invalid", e)
        None
    }
  }
}
