package pkb.sync

import javax.mail.event.{MessageCountEvent, MessageCountListener}
import javax.mail.internet.MimeMessage
import javax.mail.{FolderClosedException, _}

import akka.actor.Props
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{IRI, ValueFactory}
import pkb.actors._
import pkb.rdf.model.SimpleHashModel
import pkb.rdf.model.document.Document
import pkb.sync.Synchronizer.Sync
import pkb.sync.converter.EmailMessageConverter

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  */
object EmailSynchronizer extends Synchronizer with StrictLogging {

  private val ignoredFolderNames = Array("Junk", "Deleted", "Deleted Messages", "Spam")
  def source(valueFactory: ValueFactory) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  case class Config(store: Store)

  private class Publisher(valueFactory: ValueFactory) extends BasePublisher {

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)
    private val queue = new mutable.Queue[ImapAction]
    private val folders = new mutable.ArrayBuffer[Folder]()
    private val fetchProfile = {
      val profile = new FetchProfile()
      profile.add(FetchProfile.Item.FLAGS)
      profile.add(FetchProfile.Item.ENVELOPE)
      profile.add(FetchProfile.Item.CONTENT_INFO)
      profile
    }

    system.scheduler.schedule(1 minute, 1 minute)({
      if (waitingForData) {
        folders.foreach(_.getMessageCount) //hacky way to make servers fire MessageCountListener TODO: use IMAPFolder:idle if possible
      }
    })

    override def receive: Receive = {
      case Request(_) | Sync =>
        deliverWaitingActions()
      case config: Config =>
        onNewStore(config.store)
      case Cancel =>
        context.stop(self)
    }

    private def onNewStore(store: Store) = {
      val folders = store
        .getDefaultFolder.list("*")
        .filter(holdsInterestingMessages)

      logger.info("Importing the IMAP folders: " + folders.mkString(", "))

      folders.foreach(onNewFolder)
    }

    private def holdsInterestingMessages(folder: Folder): Boolean = {
      holdsMessages(folder) && !ignoredFolderNames.contains(folder.getName)
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
      messages.foreach(message => deliverAction(AddedMessage(message)))
    }

    private def removeMessages(messages: Array[Message], folder: Folder) = {
      messages.foreach(message => deliverAction(RemovedMessage(message)))
    }

    private def deliverAction(action: ImapAction): Unit = {
      deliverWaitingActions()

      if (waitingForData && queue.isEmpty) {
        onNext(documentForAction(action))
      } else {
        queue.enqueue(action)
      }
    }

    private def deliverWaitingActions(): Unit = {
      while (waitingForData && queue.nonEmpty) {
        onNext(documentForAction(queue.dequeue()))
      }
    }

    private def documentForAction(action: ImapAction): Document = {
      action match {
        case action: AddedMessage =>
          val context = messageContext(action.message)
          try {
            logger.info("importing message with context " + context)
            Document(context, emailMessageConverter.convert(action.message, context))
          } catch {
            case e: FolderClosedException =>
              action.message.getFolder.open(Folder.READ_ONLY)
              Document(context, emailMessageConverter.convert(action.message, context))
          }
        case action: RemovedMessage =>
          Document(messageContext(action.message), new SimpleHashModel())
      }
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    private def messageContext(message: Message): IRI = {
      message match {
        case message: MimeMessage => valueFactory.createIRI(message.getFolder.getURLName.toString + "#", message.getMessageID)
        case _ => null
      }
    }

    class ImapAction

    case class AddedMessage(message: Message) extends ImapAction

    case class RemovedMessage(message: Message) extends ImapAction
  }
}
