package thymeflow.sync

import javax.mail.event.{MessageCountEvent, MessageCountListener}
import javax.mail.internet.MimeMessage
import javax.mail.{FolderClosedException, _}

import akka.actor.Props
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{IRI, ValueFactory}
import thymeflow.actors._
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.document.Document
import thymeflow.sync.converter.EmailMessageConverter

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
    private val actionsQueue = new mutable.Queue[ImapAction]
    private val folders = new mutable.HashMap[URLName, Folder]()
    private val fetchProfile = {
      val profile = new FetchProfile()
      profile.add(FetchProfile.Item.FLAGS)
      profile.add(FetchProfile.Item.ENVELOPE)
      profile.add(FetchProfile.Item.CONTENT_INFO)
      profile
    }
    private var numberOfSent = 0

    system.scheduler.schedule(1 minute, 1 minute)({
      this.self ! Tick
    })

    override def receive: Receive = {
      case Request(_) =>
        deliverWaitingActions()
      case config: Config =>
        onNewStore(config.store)
      case Cancel =>
        context.stop(self)
      case AddedMessages(messages, folder) =>
        addMessages(messages, folder)
      case RemovedMessages(messages, folder) =>
        removeMessages(messages, folder)
      case Tick =>
        if (waitingForData) {
          folders.values.foreach(_.getMessageCount) //hacky way to make servers fire MessageCountListener TODO: use IMAPFolder:idle if possible
        }
    }

    private def onNewStore(store: Store, importFolders: Boolean = false) = {
      getStoreFolders(store).foreach(onNewFolder)
    }

    private def getStoreFolders(store: Store): Iterable[Folder] = {
      store.getDefaultFolder.list().filter(holdsInterestingMessages)
      //Seq(store.getFolder("INBOX")) //TODO: import all folders?
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
          Publisher.this.self ! AddedMessages(e.getMessages, folder)
        }

        override def messagesRemoved(e: MessageCountEvent): Unit = {
          Publisher.this.self ! RemovedMessages(e.getMessages, folder)
        }
      })

      val oldFolderOption = folders.put(folder.getURLName, folder)
      if (oldFolderOption.isEmpty) {
        importFolder(folder) //We import the folder
      } else {
        oldFolderOption.foreach(_.close(false)) //We close the old folder. It assumes that we have finished to retrieve messages from this folder
      }
    }

    private def importFolder(folder: Folder) = {
      addMessages(folder.getMessages(), folder)
    }

    private def addMessages(messages: Array[Message], folder: Folder) = {
      folder.fetch(messages, fetchProfile)
      messages.foreach(message => {
        deliverAction(AddedMessage(message))
      })
      logger.info(s"End of folder ${folder.getFullName}")
    }

    private def removeMessages(messages: Array[Message], folder: Folder) = {
      messages.foreach(message => deliverAction(RemovedMessage(message)))
    }

    private def deliverAction(action: ImapAction): Unit = {
      actionsQueue.enqueue(action)
      deliverWaitingActions()
    }

    private def deliverWaitingActions(): Unit = {
      if (!waitingForData) {
        return
      }
      val actionsQueueWasNonEmpty = actionsQueue.nonEmpty
      (1L to math.min(totalDemand, actionsQueue.size))
        .map(_ => actionsQueue.dequeue())
        .par.map(documentForAction).seq
        .foreach(onNext)
      if (actionsQueueWasNonEmpty && actionsQueue.isEmpty) {
        logger.info(s"Email importing finished with $numberOfSent messages imported")
      }
    }

    private def documentForAction(action: ImapAction): Document = {
      action match {
        case action: AddedMessage =>
          val context = messageContext(action.message)
          try {
            numberOfSent += 1
            if (numberOfSent % 100 == 0) {
              logger.info(s"$numberOfSent email messages imported")
            }
            Document(context, emailMessageConverter.convert(action.message, context))
          } catch {
            case e: FolderClosedException =>
              action.message.getFolder.open(Folder.READ_ONLY)
              Document(context, emailMessageConverter.convert(action.message, context))
          }
        case action: RemovedMessage =>
          Document(messageContext(action.message), SimpleHashModel.empty)
      }
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    private def messageContext(message: Message): IRI = {
      message match {
        case message: MimeMessage => Option(message.getMessageID).map(messageId =>
          valueFactory.createIRI(message.getFolder.getURLName.toString + "#", messageId)
        ).orNull
        case _ => null
      }
    }

    class ImapAction

    case class AddedMessage(message: Message) extends ImapAction

    case class RemovedMessage(message: Message) extends ImapAction

    case class AddedMessages(messages: Array[Message], folder: Folder)

    case class RemovedMessages(messages: Array[Message], folder: Folder)
  }

  object Tick
}
