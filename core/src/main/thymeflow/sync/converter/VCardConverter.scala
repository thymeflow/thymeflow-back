package thymeflow.sync.converter

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

import com.typesafe.scalalogging.StrictLogging
import ezvcard.parameter.{AddressType, EmailType, TelephoneType}
import ezvcard.property._
import ezvcard.util.DataUri
import ezvcard.{Ezvcard, VCard}
import org.openrdf.model._
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import thymeflow.sync.converter.utils._

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

  override def convert(stream: InputStream, context: IRI): Model = {
    convert(Ezvcard.parse(stream).all.asScala, context)
  }

  override def convert(str: String, context: IRI): Model = {
    convert(Ezvcard.parse(str).all.asScala, context)
  }

  private def convert(vCards: Traversable[VCard], context: IRI): Model = {
    val model = new SimpleHashModel(valueFactory)
    val converter = new ToModelConverter(model, context)
    vCards.foreach(vCard =>
      converter.convert(vCard)
    )
    model
  }

  private class ToModelConverter(model: Model, context: IRI) {
    def convert(vCard: VCard): Resource = {
      val cardResource = resourceFromUid(vCard.getUid)
      model.add(cardResource, RDF.TYPE, Personal.AGENT, context)

      vCard.getProperties.asScala.foreach {
        //ADR
        case address: Address => model.add(cardResource, SchemaOrg.ADDRESS, convert(address), context)
        //BDAY
        case birthday: Birthday => convert(birthday).foreach(date => model.add(cardResource, SchemaOrg.BIRTH_DATE, date, context))
        //NICKNAME
        case nicknames: Nickname =>
          nicknames.getValues.asScala.foreach(nickname =>
            model.add(cardResource, Personal.NICKNAME, valueFactory.createLiteral(nickname), context)
          )
        //DEATHDATE
        case deathDay: Deathdate => convert(deathDay).foreach(date => model.add(cardResource, SchemaOrg.DEATH_DATE, date, context))
        //EMAIL
        case email: Email =>
          convert(email).foreach {
            resource => model.add(cardResource, SchemaOrg.EMAIL, resource, context)
          }
        //FN
        case formattedName: FormattedName => model.add(cardResource, SchemaOrg.NAME, valueFactory.createLiteral(formattedName.getValue), context)
        //N
        case structuredName: StructuredName =>
          Option(structuredName.getFamily).foreach(familyName =>
            model.add(cardResource, SchemaOrg.FAMILY_NAME, valueFactory.createLiteral(familyName), context)
          )
          Option(structuredName.getGiven).foreach(givenName =>
            model.add(cardResource, SchemaOrg.GIVEN_NAME, valueFactory.createLiteral(givenName), context)
          )
          structuredName.getAdditional.asScala.foreach(additionalName =>
            model.add(cardResource, SchemaOrg.ADDITIONAL_NAME, valueFactory.createLiteral(additionalName), context)
          )
          structuredName.getPrefixes.asScala.foreach(prefix =>
            model.add(cardResource, SchemaOrg.HONORIFIC_PREFIX, valueFactory.createLiteral(prefix), context)
          )
          structuredName.getPrefixes.asScala.foreach(suffix =>
            model.add(cardResource, SchemaOrg.HONORIFIC_SUFFIX, valueFactory.createLiteral(suffix), context)
          )
        //ORG
        case organization: Organization => model.add(cardResource, SchemaOrg.MEMBER_OF, convert(organization), context)
        //PHOTO
        case photo: Photo =>
          convert(photo).foreach(photoResource =>
            model.add(cardResource, SchemaOrg.IMAGE, photoResource, context)
          )
        //REVISION
        case _: Revision => //We do not care about last revision time
        //TEL
        case telephone: Telephone =>
          convert(telephone).foreach(telephoneResource =>
            model.add(cardResource, SchemaOrg.TELEPHONE, telephoneResource, context)
          )
        //TITLE
        case title: Title => model.add(cardResource, SchemaOrg.JOB_TITLE, valueFactory.createLiteral(title.getValue), context)
        //UID
        case _: Uid => //We are already using this field to build the resource ID
        //URL
        case url: Url => convert(url).foreach(url => model.add(cardResource, SchemaOrg.URL, url, context)) //TODO: Google: support link to other accounts encoded as URLs like http\://www.google.com/profiles/112359482310702047642
        //X-SOCIALPROFILE
        case property: RawProperty if property.getPropertyName == "X-SOCIALPROFILE" =>
          resourceFromUrl(property.getValue).foreach(url => model.add(cardResource, SchemaOrg.URL, url, context)) //TODO: better relation than schema:url
        case property => logger.info("Unsupported parameter type:" + property)
      }

      cardResource
    }

    private def convert(address: Address): Resource = {
      val addressResource = uuidConverter.createBNode(address)
      model.add(addressResource, RDF.TYPE, SchemaOrg.POSTAL_ADDRESS, context)
      //TODO: getExtendedAddresses
      address.getStreetAddresses.asScala.foreach(street =>
        model.add(addressResource, SchemaOrg.STREET_ADDRESS, valueFactory.createLiteral(street), context)
      )
      address.getLocalities.asScala.foreach(locality => {
        val localityResource = uuidConverter.createBNode(addressResource.toString + "#locality-" + locality)
        model.add(localityResource, RDF.TYPE, SchemaOrg.PLACE, context)
        model.add(localityResource, SchemaOrg.NAME, valueFactory.createLiteral(locality), context)
        model.add(addressResource, SchemaOrg.ADDRESS_LOCALITY, localityResource, context)
      })
      address.getRegions.asScala.foreach(region => {
        val regionResource = uuidConverter.createBNode(addressResource.toString + "#region-" + region)
        model.add(regionResource, RDF.TYPE, SchemaOrg.PLACE, context)
        model.add(regionResource, SchemaOrg.NAME, valueFactory.createLiteral(region), context)
        model.add(addressResource, SchemaOrg.ADDRESS_REGION, regionResource, context)
      })
      address.getCountries.asScala.foreach(country => {
        val countryResource = uuidConverter.createBNode("country:" + country.toLowerCase)
        model.add(countryResource, RDF.TYPE, SchemaOrg.COUNTRY, context)
        model.add(countryResource, RDF.TYPE, SchemaOrg.PLACE, context)
        model.add(countryResource, SchemaOrg.NAME, valueFactory.createLiteral(country), context)
        model.add(addressResource, SchemaOrg.ADDRESS_COUNTRY, countryResource, context)
      })
      address.getPoBoxes.asScala.foreach(poBox =>
        model.add(addressResource, SchemaOrg.POST_OFFICE_BOX_NUMBER, valueFactory.createLiteral(poBox), context)
      )
      address.getPostalCodes.asScala.foreach(postalCode =>
        model.add(addressResource, SchemaOrg.POSTAL_CODE, valueFactory.createLiteral(postalCode), context)
      )
      address.getTypes.asScala.foreach(addressType =>
        model.add(addressResource, RDF.TYPE, classForAddressType(addressType), context)
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

    private def convert(dateTime: DateOrTimeProperty): Option[Literal] = {
      Option(dateTime.getDate).map(date => convert(date, dateTime.hasTime))
    }

    private def convert(date: Date, hasTime: Boolean): Literal = {
      val calendarDate = new GregorianCalendar()
      calendarDate.setTime(date)
      if (calendarDate.get(Calendar.YEAR) == 1970) {
        //Google use 1970 as year if there is no year set
        valueFactory.createLiteral(new SimpleDateFormat("MM-dd").format(date), XMLSchema.GMONTHDAY)
      } else if (hasTime) {
        valueFactory.createLiteral(date)
      } else {
        valueFactory.createLiteral(new SimpleDateFormat("yyyy-MM-dd").format(date), XMLSchema.DATE)
      }
    }

    private def convert(email: Email): Option[Resource] = {
      emailAddressConverter.convert(email.getValue, model).map {
        resource =>
          email.getTypes.asScala.foreach(emailType =>
            model.add(resource, RDF.TYPE, classForEmailType(emailType), context)
          )
          resource
      }
    }

    private def classForEmailType(emailType: EmailType): IRI = {
      emailType match {
        case EmailType.HOME => Personal.HOME_ADDRESS
        case EmailType.PREF => Personal.PREFERRED_ADDRESS
        case EmailType.WORK => Personal.WORK_ADDRESS
        case _ => Personal.EMAIL_ADDRESS
      }
    }

    private def convert(photo: ImageProperty): Option[Resource] = {
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
        model.add(photoResource, RDF.TYPE, SchemaOrg.IMAGE_OBJECT, context)
        photoResource
      })
    }

    private def convert(organization: Organization): Resource = {
      val organizationResource = uuidConverter.createBNode(organization)
      model.add(organizationResource, RDF.TYPE, SchemaOrg.ORGANIZATION, context)
      model.add(organizationResource, SchemaOrg.NAME, valueFactory.createLiteral(organization.getValues.get(0)), context) //TODO: support hierarchy?
      organizationResource
    }

    private def convert(telephone: Telephone): Option[Resource] = {
      Option(telephone.getUri).flatMap(uri => {
        phoneNumberConverter.convert(uri.toString, model)
      }).orElse({
        Option(telephone.getText).flatMap(rawNumber => {
          phoneNumberConverter.convert(rawNumber, model)
        })
      }).map(telephoneResource => {
        telephone.getTypes.asScala.foreach(telephoneType =>
          model.add(telephoneResource, RDF.TYPE, classForTelephoneType(telephoneType), context)
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

    private def resourceFromUrl(url: String): Option[Resource] = {
      try {
        Some(valueFactory.createIRI(url))
      } catch {
        case e: IllegalArgumentException =>
          logger.warn("The URL " + url + " is invalid", e)
          None
      }
    }

    private def resourceFromUid(uid: Uid): Resource = {
      if (uid == null) {
        valueFactory.createBNode()
      } else {
        uuidConverter.convert(uid.getValue)
      }
    }
  }
}
