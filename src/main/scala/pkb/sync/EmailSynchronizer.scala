package pkb.sync

import javax.mail.{Folder, Message, Store}

import akka.actor.Props
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.Source
import org.openrdf.model.ValueFactory
import pkb.rdf.model.document.Document
import pkb.sync.converter.EmailMessageConverter

import scala.collection.mutable

/**
  * @author Thomas Pellissier Tanon
  */
object EmailSynchronizer {

  def source(valueFactory: ValueFactory) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  case class Config(store: Store) {
  }

  private class Publisher(valueFactory: ValueFactory) extends ActorPublisher[Document] {

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)
    private val queue = new mutable.Queue[Message]

    override def receive: Receive = {
      case Request =>
        deliverWaitingMessages()
      case config: Config =>
        retrieveMessages(config.store)
    }

    private def retrieveMessages(store: Store): Unit = {
      retrieveMessages(store.getFolder("INBOX")) //TODO: discover other folders
    }

    private def retrieveMessages(folder: Folder): Unit = {
      folder.open(Folder.READ_ONLY)
      folder.getMessages.foreach(message =>
        if (waitingForData) {
          onNext(message)
        } else {
          queue.enqueue(message)
        }
      ) //TODO: don't load all light messages at the same time but do batches
      folder.close(false)
    }

    private def onNext(message: Message): Unit = {
      onNext(new Document(null, emailMessageConverter.convert(message)))
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    private def deliverWaitingMessages(): Unit = {
      while (waitingForData && queue.nonEmpty) {
        onNext(queue.dequeue())
      }
    }
  }
}
