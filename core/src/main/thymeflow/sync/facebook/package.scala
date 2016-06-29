package thymeflow.sync

import spray.json.JsonFormat
import thymeflow.sync.facebook.FacebookSynchronizer._

/**
  * @author David Montoya
  */
package object facebook {

  implicit val formatLocation = jsonFormat6(Location)
  implicit val formatPage = jsonFormat1(Page)
  implicit val formatEventPlace = jsonFormat2(EventPlace)
  implicit val formatAttendance = jsonFormat3(Invitee)
  implicit val formatEvent = jsonFormat12(Event)
  implicit val formatAgeRange = jsonFormat2(AgeRange)
  implicit val formatId = jsonFormat1(Id)
  implicit val formatMe = jsonFormat11(Me)

  implicit def formatResults[T](implicit format: JsonFormat[T]) = jsonFormat1(Results[T])

  case class Results[T](data: Vector[T])

  case class Location(city: Option[String], country: Option[String], latitude: Option[Double], longitude: Option[Double], street: Option[String], zip: Option[String])

  case class Page(name: Option[String])

  case class EventPlace(name: Option[String], location: Option[Location])

  case class Invitee(id: String, name: String, rsvp_status: String)

  case class Event(description: Option[String],
                   name: Option[String],
                   place: Option[EventPlace],
                   start_time: Option[String],
                   end_time: Option[String],
                   id: String,
                   rsvp_status: Option[String],
                   attending: Option[Results[Invitee]],
                   interested: Option[Results[Invitee]],
                   maybe: Option[Results[Invitee]],
                   noreply: Option[Results[Invitee]],
                   declined: Option[Results[Invitee]]) {
    def invited = Vector(attending, interested, maybe, noreply, declined).flatMap(x => x.toIterable.flatMap(y => y.data))
  }

  case class AgeRange(max: Option[Int], min: Option[Int])

  case class Id(id: String)

  case class Me(id: String, age_range: Option[AgeRange], bio: Option[String], birthday: Option[String], email: Option[String], first_name: Option[String], last_name: Option[String], gender: Option[String], hometown: Option[Page], updated_time: String, events: Results[Id])


}
