package thymeflow.sync.facebook

import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Model, Resource, ValueFactory}
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.vocabulary.{Personal, SchemaOrg}
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

    model.add(meNode, RDF.TYPE, Personal.AGENT, context)
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

    me.taggable_friends.data.foreach {
      taggableFriend =>
        val taggableFriendNode = valueFactory.createIRI(namespace, taggableFriend.id)

        model.add(taggableFriendNode, RDF.TYPE, Personal.AGENT, context)
        model.add(taggableFriendNode, RDF.TYPE, SchemaOrg.PERSON, context)
        taggableFriend.name.foreach {
          name =>
            model.add(taggableFriendNode, SchemaOrg.NAME, valueFactory.createLiteral(name), context)
        }
        taggableFriend.picture.foreach {
          picture =>
            picture.data.url.foreach {
              url =>
                model.add(taggableFriendNode, SchemaOrg.IMAGE, valueFactory.createLiteral(url), context)
            }
        }
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

    event.cover.foreach {
      cover =>
        cover.source.foreach {
          source =>
            model.add(eventNode, SchemaOrg.IMAGE, valueFactory.createLiteral(source), context)
        }
    }

    event.invited.foreach {
      invitee =>
        val personNode = convert(invitee, model, context)
        if (invitee.rsvp_status == "attending") {
          model.add(eventNode, SchemaOrg.ATTENDEE, personNode, context)
        }
    }

    eventNode
  }

  def convert(invitee: Invitee, model: Model, context: IRI): Resource = {
    val personNode = valueFactory.createIRI(namespace, invitee.id)

    model.add(personNode, RDF.TYPE, Personal.AGENT, context)
    model.add(personNode, RDF.TYPE, SchemaOrg.PERSON, context)
    model.add(personNode, SchemaOrg.NAME, valueFactory.createLiteral(invitee.name), context)

    personNode
  }

}
