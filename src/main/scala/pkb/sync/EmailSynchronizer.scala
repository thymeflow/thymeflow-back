package pkb.sync

import javax.mail.{Folder, Store}

import akka.actor.Props
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
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
    private val queue = new mutable.Queue[Document]

    override def receive: Receive = {
      case Request(_) =>
        deliverWaitingMessages()
      case config: Config =>
        retrieveMessages(config.store)
      case Cancel =>
        context.stop(self)
    }

    private def retrieveMessages(store: Store): Unit = {
      retrieveMessages(store.getFolder("INBOX")) //TODO: discover other folders
    }

    private def retrieveMessages(folder: Folder): Unit = {
      folder.open(Folder.READ_ONLY)
      folder.getMessages.foreach(message => {
        val document = new Document(null, emailMessageConverter.convert(message))
        if (waitingForData) {
          onNext(document)
        } else {
          queue.enqueue(document)
        }
      }) //TODO: don't load all light messages at the same time but do batches
      folder.close(false)
    }

    private def deliverWaitingMessages(): Unit = {
      while (waitingForData && queue.nonEmpty) {
        onNext(queue.dequeue())
      }
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }
  }
}
