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
import thymeflow.utilities.ExceptionUtils

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

  private class Publisher(valueFactory: ValueFactory) extends ScrollDocumentPublisher[Document, (Option[(Folder, Int, Array[Message])], Vector[Folder], Vector[Config])] {

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
          val foldersToRemove = watchedFolders.values.map {
            folder => try {
              if (!folder.isOpen) {
                folder.open(Folder.READ_ONLY)
              }
              folder.getMessageCount //hacky way to make servers fire MessageCountListener TODO: use IMAPFolder:idle if possible
              (folder, false)
            } catch {
              case e: FolderNotFoundException =>
                logger.error(s"Cannot find folder ${folder.getFullName}, skipping.")
                (folder, true)
              case e@(_: MessagingException | _: IllegalStateException) =>
                logger.error(s"Error reading folder ${folder.getFullName}, retrying later.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
                (folder, false)
            }
          }
          foldersToRemove.foreach {
            case (folder, true) =>
              watchedFolders.remove(folder.getURLName)
            case _ =>
          }
        }
    }

    override protected def queryBuilder: ((Option[(Folder, Int, Array[Message])], Vector[Folder], Vector[Config]), Long) => Future[Result] = {
      case (state@(currentOption, remainingFolders, queuedConfigs), demand) =>
        val result = currentOption match {
          case Some((currentFolder, position, messages)) =>
            try {
              val end = Math.min(position + Math.min(demand * 32L, Int.MaxValue).toInt, messages.length)
              if (!currentFolder.isOpen) {
                currentFolder.open(Folder.READ_ONLY)
              }
              val messagesToFetch = messages.slice(position, end)
              currentFolder.fetch(messagesToFetch, fetchProfile)
              val documents = messagesToFetch.flatMap(x => documentForAction(AddedMessage(x)))
              if (end == messages.length) {
                logger.info(s"Email folder ${currentFolder.getFullName} fully imported ($end messages).")
                Result(Some((None, remainingFolders, queuedConfigs)), documents)
              } else {
                Result(Some((Some((currentFolder, end, messages)), remainingFolders, queuedConfigs)), documents)
              }
            } catch {
              case e: FolderNotFoundException =>
                logger.error(s"Cannot find folder ${currentFolder.getFullName}, skipping.")
                watchedFolders.remove(currentFolder.getURLName)
                Result(Some((None, remainingFolders, queuedConfigs)), Vector.empty)
              case e@(_: MessagingException | _: IllegalStateException) =>
                logger.error(s"Error reading messages from folder ${currentFolder.getFullName}, retrying.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
                Result(Some(state), Vector.empty)
            }
          case None =>
            remainingFolders match {
              case head +: tail =>
                try {
                  processFolder(head)
                  val messages = head.getMessages
                  logger.info(s"Importing email folder ${head.getFullName} (${messages.length} messages).")
                  Result(Some((Some((head, 0, messages)), tail, queuedConfigs)), Vector.empty)
                } catch {
                  case e: FolderNotFoundException =>
                    logger.error(s"Cannot find folder ${head.getFullName}, skipping.")
                    watchedFolders.remove(head.getURLName)
                    Result(Some((None, tail, queuedConfigs)), Vector.empty)
                  case e@(_: MessagingException | _: IllegalStateException) =>
                    logger.error(s"Error opening folder ${head.getFullName}, retrying.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
                    Result(Some(state), Vector.empty)
                }
              case _ =>
                queuedConfigs match {
                  case headConfig +: tailConfigs =>
                    try {
                      val folders = getStoreFolders(headConfig.store)
                      Result(Some(None, folders, tailConfigs), Vector.empty)
                    } catch {
                      case e@(_: MessagingException | _: IllegalStateException) =>
                        logger.error(s"Error reading folders from ${headConfig.store}, skipping.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
                        Result(Some(None, Vector.empty, tailConfigs), Vector.empty)
                    }
                  case _ =>
                    Result(None, Vector.empty)
                }
            }
        }
        Future.successful(result)
    }

    private def getStoreFolders(store: Store): Vector[Folder] = {
      //TODO: import all folders?
      store.getDefaultFolder.list().filter(folderHoldsInterestingMessages).toVector
    }

    private def folderHoldsInterestingMessages(folder: Folder): Boolean = {
      folderHoldsMessages(folder) && !ignoredFolderNames.contains(folder.getName)
    }

    private def folderHoldsMessages(folder: Folder): Boolean = {
      (folder.getType & javax.mail.Folder.HOLDS_MESSAGES) != 0
    }

    private def documentForAction(action: ImapAction): Option[Document] = {
      action match {
        case action: AddedMessage =>
          val context = messageContext(action.message)
          try {
            Some(Document(context, emailMessageConverter.convert(action.message, context)))
          } catch {
            case e: FolderClosedException =>
              action.message.getFolder.open(Folder.READ_ONLY)
              Some(Document(context, emailMessageConverter.convert(action.message, context)))
            case e: MessagingException =>
              // TODO: Attempt retries
              logger.error(s"Error processing message number ${action.message.getMessageNumber}.\n${ExceptionUtils.getStackTrace(e)}")
              None
          }
        case action: RemovedMessage =>
          Some(Document(messageContext(action.message), SimpleHashModel.empty))
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
      folder.addMessageCountListener(new MessageCountListener {
        override def messagesAdded(e: MessageCountEvent): Unit = {
          Publisher.this.self ! AddedMessages(e.getMessages, folder)
        }

        override def messagesRemoved(e: MessageCountEvent): Unit = {
          Publisher.this.self ! RemovedMessages(e.getMessages, folder)
        }
      })
      folder.open(Folder.READ_ONLY)
      watchedFolders.put(folder.getURLName, folder)
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    private def addMessages(messages: Array[Message], folder: Folder) = {
      try {
        if (!folder.isOpen) {
          folder.open(Folder.READ_ONLY)
        }
        folder.fetch(messages, fetchProfile)
        buf ++= messages.flatMap(message => documentForAction(AddedMessage(message)))
        deliverBuf()
      } catch {
        case e: FolderNotFoundException =>
          logger.error(s"Cannot find folder ${folder.getFullName}, skipping.")
          watchedFolders.remove(folder.getURLName)
        case e@(_: MessagingException | _: IllegalStateException) =>
          logger.error(s"Error reading messages from folder ${folder.getFullName}, skipping.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
      }
    }

    private def removeMessages(messages: Array[Message], folder: Folder) = {
      buf ++= messages.flatMap(message => documentForAction(RemovedMessage(message)))
      deliverBuf()
    }
  }

  object Tick
}
