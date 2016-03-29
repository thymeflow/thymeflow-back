package pkb.sync

import javax.mail._
import javax.mail.event.{MessageCountEvent, MessageCountListener}
import javax.mail.internet.MimeMessage

import akka.actor.Props
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import org.openrdf.model.{IRI, ValueFactory}
import pkb.rdf.model.SimpleHashModel
import pkb.rdf.model.document.Document
import pkb.sync.Synchronizer.Sync
import pkb.sync.converter.EmailMessageConverter

import scala.collection.mutable

/**
  * @author Thomas Pellissier Tanon
  */
object EmailSynchronizer extends Synchronizer {

  def source(valueFactory: ValueFactory) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  case class Config(store: Store)

  private class Publisher(valueFactory: ValueFactory) extends BasePublisher {

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)
    private val queue = new mutable.Queue[Document]
    private val folders = new mutable.ArrayBuffer[Folder]()
    private val fetchProfile = new FetchProfile()
    fetchProfile.add(FetchProfile.Item.FLAGS)
    fetchProfile.add(FetchProfile.Item.ENVELOPE)
    fetchProfile.add(FetchProfile.Item.CONTENT_INFO)

    override def receive: Receive = {
      case Request(_) | Sync =>
        deliverDocuments()
      case config: Config =>
        onNewStore(config.store)
      case Cancel =>
        context.stop(self)
    }

    private def onNewStore(store: Store) = {
      store.getDefaultFolder.list("*")
        .filter(holdsMessages)
        .foreach(onNewFolder)
    }

    private def holdsMessages(folder: Folder): Boolean = {
      (folder.getType & javax.mail.Folder.HOLDS_MESSAGES) != 0
    }

    private def onNewFolder(folder: Folder) = {
      folder.open(Folder.READ_ONLY)
      folder.addMessageCountListener(new MessageCountListener {
        override def messagesAdded(e: MessageCountEvent): Unit = {
          addMessages(e.getMessages, folder)
        }

        override def messagesRemoved(e: MessageCountEvent): Unit = {
          removeMessages(e.getMessages, folder)
        }
      })
      addMessages(folder.getMessages(), folder)
    }

    private def addMessages(messages: Array[Message], folder: Folder) = {
      folder.fetch(messages, fetchProfile)
      messages.foreach(message => {
        val context = messageContext(message, folder)
        deliverDocument(Document(context, emailMessageConverter.convert(message, context)))
      })
    }

    private def removeMessages(messages: Array[Message], folder: Folder) = {
      messages.foreach(message =>
        deliverDocument(Document(messageContext(message, folder), new SimpleHashModel()))
      )
    }

    private def deliverDocuments(): Unit = {
      deliverWaitingDocuments()
      if (waitingForData) {
        folders.foreach(_.getMessageCount) //hacky way to make servers fire MessageCountListener TODO: use IMAPFolder:idle if possible
      }
    }

    private def deliverDocument(document: Document): Unit = {
      deliverWaitingDocuments()
      if (waitingForData && queue.isEmpty) {
        onNext(document)
      } else {
        queue.enqueue(document)
      }
    }

    private def deliverWaitingDocuments(): Unit = {
      while (waitingForData && queue.nonEmpty) {
        onNext(queue.dequeue())
      }
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    private def messageContext(message: Message, folder: Folder): IRI = {
      message match {
        case message: MimeMessage => valueFactory.createIRI(folder.getURLName.toString + "#", message.getMessageID)
        case _ => null
      }
    }
  }
}
