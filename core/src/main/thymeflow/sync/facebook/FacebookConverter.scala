package thymeflow.sync.facebook

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, Resource, ValueFactory}
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.vocabulary.SchemaOrg
import thymeflow.sync.converter.utils.EmailAddressConverter

/**
  * @author David Montoya
  */
class FacebookConverter(valueFactory: ValueFactory) extends StrictLogging {

  final val namespace = "https://graph.facebook.com/"
  private val converter = new EmailAddressConverter(valueFactory)

  def convert(me: Me, context: IRI): Model = {
    val model = new SimpleHashModel()
    val meNode = valueFactory.createIRI(namespace, me.id)

    model.add(meNode, RDF.TYPE, SchemaOrg.PERSON, context)

    me.birthday.foreach {
      birthday =>
        model.add(meNode, SchemaOrg.BIRTH_DATE, valueFactory.createLiteral(birthday), context)
    }

    me.first_name.foreach {
      firstName =>
        model.add(meNode, SchemaOrg.GIVEN_NAME, valueFactory.createLiteral(firstName), context)
    }

    me.last_name.foreach {
      lastName =>
        model.add(meNode, SchemaOrg.FAMILY_NAME, valueFactory.createLiteral(lastName), context)
    }

    me.gender.foreach {
      gender =>
        model.add(meNode, SchemaOrg.GENDER, valueFactory.createLiteral(gender), context)
    }

    me.email.foreach {
      email =>
        converter.convert(email, model).foreach {
          emailNode =>
            model.add(meNode, SchemaOrg.EMAIL, emailNode, context)
        }
    }

    me.bio.foreach {
      bio =>
        model.add(meNode, SchemaOrg.DESCRIPTION, valueFactory.createLiteral(bio), context)
    }

    model
  }

  def convert(event: Event, model: Model, context: IRI): Resource = {
    val eventNode = valueFactory.createIRI(namespace, event.id)
    model.add(eventNode, RDF.TYPE, SchemaOrg.EVENT, context)
    event.start_time.foreach {
      startTime =>
        model.add(eventNode, SchemaOrg.START_DATE, valueFactory.createLiteral(startTime), context)
    }

    event.end_time.foreach {
      endTime =>
        model.add(eventNode, SchemaOrg.END_DATE, valueFactory.createLiteral(endTime), context)
    }

    event.description.foreach {
      description =>
        model.add(eventNode, SchemaOrg.DESCRIPTION, valueFactory.createLiteral(description), context)
    }

    event.name.foreach {
      name =>
        model.add(eventNode, SchemaOrg.NAME, valueFactory.createLiteral(name), context)
    }

    event.invited.foreach {
      invitee =>
        convert(eventNode, invitee, model, context)
    }

    eventNode
  }

  def convert(eventNode: Resource, attendance: Invitee, model: Model, context: IRI): Resource = {
    val personNode = valueFactory.createIRI(namespace, attendance.id)

    model.add(personNode, RDF.TYPE, SchemaOrg.PERSON, context)
    model.add(personNode, SchemaOrg.NAME, valueFactory.createLiteral(attendance.name), context)

    personNode
  }

}
