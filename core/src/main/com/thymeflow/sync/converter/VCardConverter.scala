package com.thymeflow.sync.converter

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

import com.thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
import com.thymeflow.rdf.model.{StatementSet, StatementSetDiff}
import com.thymeflow.spatial
import com.thymeflow.sync.converter.utils._
import com.thymeflow.update.{UpdateResult, UpdateResults}
import com.thymeflow.utilities.{Error, Ok}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import ezvcard.parameter.{AddressType, EmailType, ImageType, TelephoneType}
import ezvcard.property._
import ezvcard.util.{DataUri, TelUri}
import ezvcard.{Ezvcard, VCard}
import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.vocabulary.{RDF, XMLSchema}

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
class VCardConverter(valueFactory: ValueFactory)(implicit config: Config) extends Converter with StrictLogging {

  private val emailAddressConverter = new EmailAddressConverter(valueFactory)
  // TODO: Guess the phone number region
  private val phoneNumberConverter = new PhoneNumberConverter(valueFactory, config.getString("thymeflow.converter.vcard.phone-number-default-region"))
  private val postalAddressConverter = new PostalAddressConverter(valueFactory)
  private val uuidConverter = new UUIDConverter(valueFactory)

  override def convert(stream: InputStream, context: Option[String] => IRI, createSourceContext: (StatementSet, String) => IRI): Iterator[(IRI, StatementSet)] = {
    val wholeContext = context(None)
    Iterator((wholeContext, convert(Ezvcard.parse(stream).all.asScala, wholeContext)))
  }

  def convert(str: String, context: Resource): StatementSet = {
    convert(Ezvcard.parse(str).all.asScala, context)
  }

  private def convert(vCards: Traversable[VCard], context: Resource): StatementSet = {
    val statements = StatementSet.empty(valueFactory)
    val converter = new ToModelConverter(statements, context)
    vCards.foreach(converter.convert)
    statements
  }

  override def applyDiff(str: String, diff: StatementSetDiff): (String, UpdateResults) = {
    val results = Ezvcard.parse(str).all().asScala.map(vCard => {
      val diffApplication = new DiffApplication(vCard)
      val removed = diff.removed.flatMap(statement =>
        diffApplication.remove(statement).map(statement -> _)
      )(scala.collection.breakOut): Map[Statement, UpdateResult]
      val added = diff.added.flatMap(statement =>
        diffApplication.add(statement).map(statement -> _)
      )(scala.collection.breakOut): Map[Statement, UpdateResult]
      (UpdateResults(added, removed), vCard)
    }).toVector
    val written = Ezvcard.write(results.map(_._2).asJava).go()
    (written, UpdateResults.merge(results.map(_._1): _*))
  }

  private def resourceForVCard(vCard: VCard): Resource = {
    Option(vCard.getUid)
      .map(uid => uuidConverter.convert(uid.getValue))
      .getOrElse(uuidConverter.createIRI(vCard))
  }

  private class ToModelConverter(statements: StatementSet, context: Resource) {
    def convert(vCard: VCard): Resource = {
      val cardResource = resourceForVCard(vCard)
      statements.add(cardResource, RDF.TYPE, Personal.AGENT, context)

      vCard.getProperties.asScala.foreach {
        //ADR
        case address: Address => statements.add(cardResource, SchemaOrg.ADDRESS, convert(address), context)
        //BDAY
        case birthday: Birthday => convert(birthday).foreach(date => statements.add(cardResource, SchemaOrg.BIRTH_DATE, date, context))
        //NICKNAME
        case nicknames: Nickname =>
          nicknames.getValues.asScala.foreach(nickname =>
            statements.add(cardResource, Personal.NICKNAME, valueFactory.createLiteral(nickname), context)
          )
        //DEATHDATE
        case deathdate: Deathdate => convert(deathdate).foreach(date => statements.add(cardResource, SchemaOrg.DEATH_DATE, date, context))
        //EMAIL
        case email: Email =>
          convert(email).foreach {
            resource => statements.add(cardResource, SchemaOrg.EMAIL, resource, context)
          }
        //FN
        case formattedName: FormattedName => statements.add(cardResource, SchemaOrg.NAME, convert(formattedName), context)
        //N
        case structuredName: StructuredName =>
          Option(structuredName.getFamily).foreach(familyName =>
            statements.add(cardResource, SchemaOrg.FAMILY_NAME, valueFactory.createLiteral(familyName), context)
          )
          Option(structuredName.getGiven).foreach(givenName =>
            statements.add(cardResource, SchemaOrg.GIVEN_NAME, valueFactory.createLiteral(givenName), context)
          )
          structuredName.getAdditionalNames.asScala.foreach(additionalName =>
            statements.add(cardResource, SchemaOrg.ADDITIONAL_NAME, valueFactory.createLiteral(additionalName), context)
          )
          structuredName.getPrefixes.asScala.foreach(prefix =>
            statements.add(cardResource, SchemaOrg.HONORIFIC_PREFIX, valueFactory.createLiteral(prefix), context)
          )
          structuredName.getSuffixes.asScala.foreach(suffix =>
            statements.add(cardResource, SchemaOrg.HONORIFIC_SUFFIX, valueFactory.createLiteral(suffix), context)
          )
        //ORG
        case organization: Organization => statements.add(cardResource, SchemaOrg.MEMBER_OF, convert(organization), context)
        //PHOTO
        case photo: Photo =>
          convert(photo).foreach(photoResource =>
            statements.add(cardResource, SchemaOrg.IMAGE, photoResource, context)
          )
        //REVISION
        case _: Revision => //We do not care about last revision time
        //TEL
        case telephone: Telephone =>
          convert(telephone).foreach(telephoneResource =>
            statements.add(cardResource, SchemaOrg.TELEPHONE, telephoneResource, context)
          )
        //TITLE
        case title: Title => statements.add(cardResource, SchemaOrg.JOB_TITLE, valueFactory.createLiteral(title.getValue), context)
        //UID
        case _: Uid => //We are already used this field to build the resource ID
        //URL
        case url: Url => convert(url).foreach(url => statements.add(cardResource, SchemaOrg.URL, url, context)) //TODO: Google: support link to other accounts encoded as URLs like http\://www.google.com/profiles/112359482310702047642
        //X-SOCIALPROFILE
        case property: RawProperty if property.getPropertyName == "X-SOCIALPROFILE" =>
          resourceFromUrl(property.getValue).foreach(url => statements.add(cardResource, SchemaOrg.URL, url, context)) //TODO: better relation than schema:url
        case property => logger.info("Unsupported parameter type:" + property)
      }

      cardResource
    }

    private def convert(address: Address): Resource = {
      val addressResource = postalAddressConverter.convert(spatial.SimpleAddress(
        street = Option(address.getStreetAddressFull),
        locality = Option(address.getLocality),
        region = Option(address.getRegion),
        country = Option(address.getCountry),
        postalCode = Option(address.getPostalCode)
      ), statements, Some(context))
      address.getPoBoxes.asScala.foreach(poBox =>
        statements.add(addressResource, SchemaOrg.POST_OFFICE_BOX_NUMBER, valueFactory.createLiteral(poBox), context)
      )
      address.getTypes.asScala.foreach(addressType =>
        statements.add(addressResource, RDF.TYPE, classForAddressType(addressType), context)
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
      emailAddressConverter.convert(email.getValue, statements).map {
        resource =>
          email.getTypes.asScala.foreach(emailType =>
            statements.add(resource, RDF.TYPE, classForEmailType(emailType), context)
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

    private def convert(formattedName: FormattedName): Literal = {
      if (formattedName.getLanguage == null) {
        valueFactory.createLiteral(formattedName.getValue)
      } else {
        valueFactory.createLiteral(formattedName.getValue, formattedName.getLanguage)
      }
    }

    private def convert(photo: ImageProperty): Option[Resource] = {
      Option(photo.getUrl).map(uri => {
        valueFactory.createIRI(uri)
      }).orElse({
        Option(photo.getData).flatMap(binary => {
          Option(photo.getContentType).orElse(VCardImageType.guess(binary)).map(contentType => {
            valueFactory.createIRI(new DataUri(contentType.getMediaType, binary).toString)
          }).orElse({
            logger.warn("Photo with unknwon content type.")
            None
          })
        })
      }).map(photoResource => {
        statements.add(photoResource, RDF.TYPE, SchemaOrg.IMAGE_OBJECT, context)
        photoResource
      })
    }

    private def convert(organization: Organization): Resource = {
      val organizationResource = uuidConverter.createBNode(organization)
      statements.add(organizationResource, RDF.TYPE, SchemaOrg.ORGANIZATION, context)
      statements.add(organizationResource, SchemaOrg.NAME, valueFactory.createLiteral(organization.getValues.get(0)), context) //TODO: support hierarchy?
      organizationResource
    }

    private def convert(telephone: Telephone): Option[Resource] = {
      Option(telephone.getUri).flatMap(uri => {
        phoneNumberConverter.convert(uri.toString, statements)
      }).orElse(
        Option(telephone.getText).flatMap(phoneNumberConverter.convert(_, statements))
      ).map(telephoneResource => {
        telephone.getTypes.asScala.foreach(telephoneType =>
          statements.add(telephoneResource, RDF.TYPE, classForTelephoneType(telephoneType), context)
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
  }

  private class DiffApplication(vCard: VCard) {
    //TODO: drama: what to do for (agent, schema:organization, org) /\ (org, schema:name, name) ???
    //The add method should always be called after remove in case of edit
    def add(statement: Statement): Option[UpdateResult] = {
      if (statement.getSubject == resourceForVCard(vCard)) {
        try {
          addCardStatement(statement)
          Some(Ok())
        } catch {
          case e: ConverterException => Some(Error(List(e)))
        }
      } else {
        None
      }
    }

    private def addCardStatement(statement: Statement): Boolean = {
      //TODO: we do not support alternative representation for structured name (only VCard 4.0)
      def getOrCreateStructuredName() = {
        Option(vCard.getStructuredName).getOrElse({
          val name = new StructuredName()
          vCard.setStructuredName(name)
          name
        })
      }
      statement.getPredicate match {
        case SchemaOrg.ADDITIONAL_NAME =>
          getOrCreateStructuredName().getAdditionalNames.add(statement.getObject.stringValue())
        case SchemaOrg.BIRTH_DATE => vCard.getBirthdays.add(toBirthdayProperty(statement.getObject))
        case SchemaOrg.DEATH_DATE => vCard.getDeathdates.add(toDeathdateProperty(statement.getObject))
        case SchemaOrg.EMAIL => vCard.getEmails.add(toEmailProperty(statement.getObject))
        case SchemaOrg.FAMILY_NAME =>
          val structuredName = getOrCreateStructuredName()
          val newFamily = statement.getObject.stringValue()
          Option(structuredName.getFamily).filter(_ != newFamily).foreach {
            _ => throw new ConverterException("The family name is already set to a different value")
          }
          structuredName.setFamily(newFamily)
          true
        case SchemaOrg.GIVEN_NAME =>
          val structuredName = getOrCreateStructuredName()
          val newGiven = statement.getObject.stringValue()
          Option(structuredName.getGiven).filter(_ != newGiven).foreach {
            _ => throw new ConverterException("The given name is already set to a different value")
          }
          structuredName.setGiven(newGiven)
          true
        case SchemaOrg.HONORIFIC_PREFIX =>
          getOrCreateStructuredName().getPrefixes.add(statement.getObject.stringValue())
        case SchemaOrg.HONORIFIC_SUFFIX =>
          getOrCreateStructuredName().getSuffixes.add(statement.getObject.stringValue())
        case SchemaOrg.IMAGE => vCard.getPhotos.add(toPhotoProperty(statement.getObject))
        case SchemaOrg.JOB_TITLE => vCard.getTitles.add(toTitleProperty(statement.getObject))
        case SchemaOrg.NAME => vCard.getFormattedNames.add(toFormattedNameProperty(statement.getObject))
        case Personal.NICKNAME =>
          vCard.getNicknames.asScala.headOption.getOrElse(new Nickname()).getValues.add(statement.getObject.stringValue())
        case SchemaOrg.TELEPHONE => vCard.getTelephoneNumbers.add(toTelephoneProperty(statement.getObject))
        case SchemaOrg.URL => vCard.addUrl(statement.getObject.stringValue()); true
        //SchemaOrg.MEMBER_OF
        case _ => throw new ConverterException(s"Unsupported property for vCard addition ${statement.getPredicate}")
      }
    }

    def remove(statement: Statement): Option[UpdateResult] = {
      if (statement.getSubject == resourceForVCard(vCard)) {
        try {
          removeCardStatement(statement)
          Some(Ok())
        } catch {
          case e: ConverterException => Some(Error(List(e)))
        }
      } else {
        None
      }
    }

    private def removeCardStatement(statement: Statement): Boolean = {
      statement.getPredicate match {
        case SchemaOrg.ADDITIONAL_NAME =>
          vCard.getStructuredNames.asScala.exists(_.getAdditionalNames.remove(statement.getObject.stringValue()))
        case SchemaOrg.BIRTH_DATE => vCard.removeProperty(toBirthdayProperty(statement.getObject))
        case SchemaOrg.DEATH_DATE => vCard.removeProperty(toDeathdateProperty(statement.getObject))
        case SchemaOrg.EMAIL => vCard.removeProperty(toEmailProperty(statement.getObject))
        case SchemaOrg.FAMILY_NAME =>
          val oldFamily = statement.getObject.stringValue()
          val structuredNames = vCard.getStructuredNames.asScala.filter(_.getFamily == oldFamily)
          structuredNames.foreach(_.setFamily(null))
          structuredNames.nonEmpty
        case SchemaOrg.GIVEN_NAME =>
          val oldGiven = statement.getObject.stringValue()
          val structuredNames = vCard.getStructuredNames.asScala.filter(_.getGiven == oldGiven)
          structuredNames.foreach(_.setGiven(null))
          structuredNames.nonEmpty
        case SchemaOrg.HONORIFIC_PREFIX =>
          vCard.getStructuredNames.asScala.exists(_.getPrefixes.remove(statement.getObject.stringValue()))
        case SchemaOrg.HONORIFIC_SUFFIX =>
          vCard.getStructuredNames.asScala.exists(_.getSuffixes.remove(statement.getObject.stringValue()))
        case SchemaOrg.IMAGE => vCard.removeProperty(toPhotoProperty(statement.getObject))
        case SchemaOrg.JOB_TITLE => vCard.removeProperty(toTitleProperty(statement.getObject))
        case SchemaOrg.NAME => vCard.removeProperty(toFormattedNameProperty(statement.getObject))
        case Personal.NICKNAME =>
          vCard.getNicknames.asScala.exists(_.getValues.remove(statement.getObject.stringValue()))
        case SchemaOrg.TELEPHONE =>
          vCard.getTelephoneNumbers.asScala.exists(telephone => {
            Option(telephone.getUri.toString)
              .orElse(Option(telephone.getText))
              .flatMap(phoneNumberConverter.buildTelUri)
              .filter(_ == statement.getObject.stringValue())
              .exists(_ => vCard.removeProperty(telephone))
          })
        case SchemaOrg.URL =>
          vCard.removeProperty(new Url(statement.getObject.stringValue()))
          vCard.removeProperty(new RawProperty("X-SOCIALPROFILE", statement.getObject.stringValue()))
        case _ => throw new ConverterException(s"Unsupported property for vCard deletion ${statement.getPredicate}")
      }
    }

    private def toBirthdayProperty(value: Value): Birthday = {
      value match {
        case literal: Literal if literal.getDatatype == XMLSchema.DATETIME =>
          new Birthday(literal.calendarValue().toGregorianCalendar.getTime, true)
        case literal: Literal if literal.getDatatype == XMLSchema.DATE =>
          new Birthday(literal.calendarValue().toGregorianCalendar.getTime)
        case _ => throw new ConverterException(s"$value should be a valid date or dateTime literal")
      }
    }


    private def toDeathdateProperty(value: Value): Deathdate = {
      value match {
        case literal: Literal if literal.getDatatype == XMLSchema.DATETIME =>
          new Deathdate(literal.calendarValue().toGregorianCalendar.getTime, true)
        case literal: Literal if literal.getDatatype == XMLSchema.DATE =>
          new Deathdate(literal.calendarValue().toGregorianCalendar.getTime)
        case _ => throw new ConverterException(s"$value should be a valid date or dateTime literal")
      }
    }

    private def toEmailProperty(value: Value): Email = {
      value match {
        case iri: IRI if iri.stringValue.startsWith("mailto:") => new Email(iri.getLocalName)
        case _ => throw new ConverterException(s"$value should be a valid mailto: IRI")
      }
    }


    private def toFormattedNameProperty(value: Value): FormattedName = {
      value match {
        case literal: Literal =>
          val formattedName = new FormattedName(literal.stringValue())
          literal.getLanguage.asScala.foreach(formattedName.setLanguage)
          formattedName
        case _ => throw new ConverterException(s"$value should be a string literal")
      }
    }

    private def toTitleProperty(value: Value): Title = {
      value match {
        case literal: Literal =>
          val title = new Title(literal.stringValue())
          literal.getLanguage.asScala.foreach(title.setLanguage)
          title
        case _ => throw new ConverterException(s"$value should be a string literal")
      }
    }

    private def toPhotoProperty(value: Value): Photo = {
      //TODO: should we add data: IRI or string?
      value match {
        case iri: IRI if iri.stringValue.startsWith("data:") =>
          val data = DataUri.parse(iri.stringValue())
          new Photo(data.getData, ImageType.find(null, data.getContentType, null))
        case iri: IRI =>
          val uri = iri.stringValue()
          new Photo(uri, ImageType.find(null, null, uri.substring(uri.lastIndexOf(".") + 1)))
        case _ => throw new ConverterException(s"$value should be a data: IRI or an IRI with an image file extension")
      }
    }

    private def toTelephoneProperty(value: Value): Telephone = {
      value match {
        case iri: IRI if iri.stringValue.startsWith("tel:") => new Telephone(TelUri.parse(iri.stringValue()))
        case _ => throw new ConverterException(s"$value should be a valid tel: IRI")
      }
    }
  }
}
