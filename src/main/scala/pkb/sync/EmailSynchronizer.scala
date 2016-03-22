package pkb.sync

import javax.mail.{Folder, Store}

import akka.actor.Props
import akka.stream.scaladsl.Source
import org.openrdf.model.ValueFactory
import pkb.rdf.model.document.Document
import pkb.sync.converter.EmailMessageConverter
import pkb.sync.publisher.ScrollDocumentPublisher

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * @author Thomas Pellissier Tanon
  */
object EmailSynchronizer extends Synchronizer {

  def source(valueFactory: ValueFactory) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  case class Config(store: Store)

  private class Publisher(valueFactory: ValueFactory)
    extends ScrollDocumentPublisher[Document, (Vector[Config], Option[(Folder, Int, Int)])] with BasePublisher {

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)

    override def receive: Receive = {
      case config: Config =>
        currentScrollOption = Some(currentScrollOption match {
          case Some((queuedConfigs, nextMessageOption)) =>
            (queuedConfigs :+ config, nextMessageOption)
          case None =>
            (Vector(config), None)
        })
        nextResults(totalDemand)
      case message =>
        super.receive(message)
    }

    override protected def queryBuilder = {
      case ((queuedConfigs, Some((folder, messageIndex, messageCount))), demand) =>
        Future {
          val message = folder.getMessage(messageIndex)
          val model = emailMessageConverter.convert(message, null)
          val document = Document(null, model)
          // indexes go from 1 to messageCount
          val nextMessageOption = if (messageIndex + 1 <= messageCount) {
            Some((folder, messageIndex + 1, messageCount))
          } else {
            None
          }
          Result(scroll = Some((queuedConfigs, nextMessageOption)), hits = Some(document))
        }
      case ((nextConfig +: tail, None), _) =>
        Future {
          val folder = nextConfig.store.getFolder("INBOX") //TODO: discover other folders
          folder.open(Folder.READ_ONLY)
          folder.getMessages
          val messageCount = folder.getMessageCount
          // indexes go from 1 to messageCount
          val nextMessageOption = if (messageCount > 0) {
            Some((folder, 1, messageCount))
          } else {
            None
          }
          Result(scroll = Some((tail, nextMessageOption)), hits = None)
        }
      case ((Vector(), None), _) =>
        Future.successful(Result(scroll = None, hits = None))
    }


  }
}
