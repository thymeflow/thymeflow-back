package thymeflow.sync

import javax.mail.event.{MessageCountEvent, MessageCountListener}
import javax.mail.internet.MimeMessage
import javax.mail.{FolderClosedException, _}

import akka.actor.Props
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{IRI, ValueFactory}
import thymeflow.actors._
import thymeflow.rdf.model.SimpleHashModel
import thymeflow.rdf.model.document.Document
import thymeflow.sync.converter.EmailMessageConverter
import thymeflow.sync.publisher.ScrollDocumentPublisher

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object EmailSynchronizer extends Synchronizer with StrictLogging {

  private val ignoredFolderNames = Array("Junk", "Deleted", "Deleted Messages", "Spam")

  def source(valueFactory: ValueFactory) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  sealed trait ImapAction

  case class Config(store: Store)

  case class AddedMessage(message: Message) extends ImapAction

  case class RemovedMessage(message: Message) extends ImapAction

  case class AddedMessages(messages: Array[Message], folder: Folder)

  case class RemovedMessages(messages: Array[Message], folder: Folder)

  private class Publisher(valueFactory: ValueFactory) extends ScrollDocumentPublisher[Document, (Option[(Folder, Int, Int)], Vector[Folder], Vector[Config])] {

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)
    private val watchedFolders = new mutable.HashMap[URLName, Folder]()
    private val fetchProfile = {
      val profile = new FetchProfile()
      profile.add(FetchProfile.Item.FLAGS)
      profile.add(FetchProfile.Item.ENVELOPE)
      profile.add(FetchProfile.Item.CONTENT_INFO)
      profile.add("In-Reply-To")
      profile
    }
    private var numberOfEmailMessageAdded = 0

    system.scheduler.schedule(1 minute, 1 minute)({
      this.self ! Tick
    })

    override def receive: Receive = super.receive orElse {
      case config: Config =>
        currentScrollOption = Some(currentScrollOption match {
          case Some((currentFolderOption, folders, queuedConfigs)) =>
            (currentFolderOption, folders, queuedConfigs :+ config)
          case None =>
            (None, Vector.empty, Vector(config))
        })
        nextResults(totalDemand)
      case AddedMessages(messages, folder) =>
        addMessages(messages, folder)
      case RemovedMessages(messages, folder) =>
        removeMessages(messages, folder)
      case Tick =>
        if (currentScrollOption.isEmpty && waitingForData) {
          watchedFolders.values.foreach(_.getMessageCount) //hacky way to make servers fire MessageCountListener TODO: use IMAPFolder:idle if possible
        }
    }

    override protected def queryBuilder: ((Option[(Folder, Int, Int)], Vector[Folder], Vector[Config]), Long) => Future[Result] = {
      case ((currentOption, remainingFolders, queuedConfigs), demand) =>
        val result = currentOption match {
          case Some((currentFolder, position, max)) =>
            val end = Math.min(position + Math.min(demand * 16L, Int.MaxValue).toInt - 1, max)
            val messages = currentFolder.getMessages(position, end)
            currentFolder.fetch(messages, fetchProfile)
            val documents = messages.map(x => documentForAction(AddedMessage(x)))
            if (end == max) {
              logger.info(s"Email folder ${currentFolder.getFullName} fully imported ($max messages).")
              Result(Some((None, remainingFolders, queuedConfigs)), documents)
            } else {
              Result(Some((Some((currentFolder, end + 1, max)), remainingFolders, queuedConfigs)), documents)
            }
          case None =>
            remainingFolders match {
              case head +: tail =>
                processFolder(head)
                val messageCount = head.getMessageCount
                logger.info(s"Importing email folder ${head.getFullName} fully imported ($messageCount messages).")
                Result(Some((Some((head, 1, messageCount)), tail, queuedConfigs)), Vector.empty)
              case _ =>
                queuedConfigs match {
                  case headConfig +: tailConfigs =>
                    val folders = getStoreFolders(headConfig.store)
                    Result(Some(None, folders, tailConfigs), Vector.empty)
                  case _ =>
                    logger.info(s"Email import finished with $numberOfEmailMessageAdded messages imported.")
                    Result(None, Vector.empty)
                }
            }
        }
        Future.successful(result)
    }

    private def getStoreFolders(store: Store): Vector[Folder] = {
      //TODO: import all folders?
      store.getDefaultFolder.list().filter(holdsInterestingMessages).toVector
    }

    private def holdsInterestingMessages(folder: Folder): Boolean = {
      holdsMessages(folder) && !ignoredFolderNames.contains(folder.getName)
    }

    private def holdsMessages(folder: Folder): Boolean = {
      (folder.getType & javax.mail.Folder.HOLDS_MESSAGES) != 0
    }

    private def documentForAction(action: ImapAction): Document = {
      action match {
        case action: AddedMessage =>
          val context = messageContext(action.message)
          try {
            numberOfEmailMessageAdded += 1
            if (numberOfEmailMessageAdded % 100 == 0) {
              logger.info(s"$numberOfEmailMessageAdded email messages imported")
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

    private def messageContext(message: Message): IRI = {
      message match {
        case message: MimeMessage => Option(message.getMessageID).map(messageId =>
          valueFactory.createIRI(message.getFolder.getURLName.toString + "#", messageId)
        ).orNull
        case _ => null
      }
    }

    def processFolder(folder: Folder) = {
      folder.open(Folder.READ_ONLY)

      folder.addMessageCountListener(new MessageCountListener {
        override def messagesAdded(e: MessageCountEvent): Unit = {
          Publisher.this.self ! AddedMessages(e.getMessages, folder)
        }

        override def messagesRemoved(e: MessageCountEvent): Unit = {
          Publisher.this.self ! RemovedMessages(e.getMessages, folder)
        }
      })
      watchedFolders.put(folder.getURLName, folder)
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    private def addMessages(messages: Array[Message], folder: Folder) = {
      folder.fetch(messages, fetchProfile)
      buf ++= messages.map(message => documentForAction(AddedMessage(message)))
      deliverBuf()
    }

    private def removeMessages(messages: Array[Message], folder: Folder) = {
      buf ++= messages.map(message => documentForAction(RemovedMessage(message)))
      deliverBuf()
    }
  }

  object Tick
}
