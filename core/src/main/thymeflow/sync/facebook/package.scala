package thymeflow.sync

import spray.json.JsonFormat
import thymeflow.sync.facebook.FacebookSynchronizer._

/**
  * @author David Montoya
  */
package object facebook {

  implicit def formatResults[T](implicit format: JsonFormat[T]) = jsonFormat1(Results[T])

  implicit def formatResult[T](implicit format: JsonFormat[T]) = jsonFormat1(Result[T])

  implicit val formatLocation = jsonFormat8(Location)
  implicit val formatPage = jsonFormat1(Page)
  implicit val formatEventPlace = jsonFormat3(EventPlace)
  implicit val formatAttendance = jsonFormat3(Invitee)
  implicit val formatCovert = jsonFormat2(Cover)
  implicit val formatEvent = jsonFormat13(Event)
  implicit val formatAgeRange = jsonFormat2(AgeRange)
  implicit val formatId = jsonFormat1(Id)
  implicit val formatPicture = jsonFormat2(Picture)
  implicit val formatTaggableFriend = jsonFormat3(TaggableFriend)
  implicit val formatMe = jsonFormat12(Me)

  case class Results[T](data: Vector[T])

  case class Location(city: Option[String], country: Option[String], latitude: Option[Double], longitude: Option[Double], region: Option[String], state: Option[String], street: Option[String], zip: Option[String])

  case class Page(name: Option[String])

  case class EventPlace(id: Option[String], name: Option[String], location: Option[Location])

  case class Invitee(id: String, name: String, rsvp_status: String)

  case class Event(description: Option[String],
                   name: Option[String],
                   place: Option[EventPlace],
                   cover: Option[Cover],
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

  case class Cover(id: String, source: Option[String])

  case class Picture(url: Option[String], is_silhouette: Option[Boolean])

  case class Result[T](data: T)

  case class TaggableFriend(id: String, name: Option[String], picture: Option[Result[Picture]])

  case class Me(id: String, age_range: Option[AgeRange], bio: Option[String], birthday: Option[String], email: Option[String], first_name: Option[String], last_name: Option[String], gender: Option[String], hometown: Option[Page], updated_time: String, events: Results[Id], taggable_friends: Results[TaggableFriend])
}
