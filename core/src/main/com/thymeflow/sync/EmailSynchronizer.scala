package com.thymeflow.sync

import java.util.concurrent.TimeUnit
import javax.mail.event._
import javax.mail.{FolderClosedException, _}

import akka.actor.Props
import akka.stream.scaladsl.Source
import com.sun.mail.imap.{IMAPFolder, IMAPMessage}
import com.thymeflow.actors._
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.sync.converter.{ConverterException, EmailMessageConverter}
import com.thymeflow.sync.publisher.ScrollDocumentPublisher
import com.thymeflow.update.UpdateResults
import com.thymeflow.utilities.{ExceptionUtils, TimeExecution}
import com.typesafe.config.{Config => AppConfig}
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{IRI, ValueFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object EmailSynchronizer extends Synchronizer with StrictLogging {

  // Prevent memory leaks due to caching of Messages' multipart content.
  System.setProperty("mail.mime.cachemultipart", "false")

  private final val retryTime = Duration(15, TimeUnit.SECONDS)
  private final val fetchedMessagesMaxBufferSize = 512
  private final val fetchedMessagesDemandMultiplier = 32L
  private val ignoredFolderNames = Set("Junk", "Deleted", "Deleted Messages", "Spam")

  def source(valueFactory: ValueFactory)(implicit appConfig: AppConfig) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory)))

  sealed trait ImapAction {
    def uidValidity: Long

    def messageUid: Long

    def folderURLName: URLName
  }

  case class Config(store: Store, folderNamesToKeep: Option[Set[String]] = None)

  case class AddedMessage(uidValidity: Long, messageUid: Long, message: Message) extends ImapAction {
    def folderURLName = message.getFolder.getURLName
  }

  case class RemovedMessage(folderURLName: URLName, uidValidity: Long, messageUid: Long) extends ImapAction

  case class UpdatedMessages(added: Boolean, folder: IMAPFolder, messages: Vector[IMAPMessage])

  case class ConnectionClosed(folder: IMAPFolder)

  private class Publisher(valueFactory: ValueFactory)(implicit appConfig: AppConfig) extends ScrollDocumentPublisher[Document, (Vector[(IMAPFolder, Option[(Boolean, Vector[IMAPMessage])])], Vector[Config])] {

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)
    private val watchedFolders = new mutable.HashMap[URLName, (IMAPFolder, Long, mutable.HashSet[Long], (MessageCountListener, ConnectionListener))]()

    private val uidFetchProfile = {
      val profile = new FetchProfile()
      profile.add(UIDFolder.FetchProfileItem.UID)
      profile
    }

    private val fetchProfile = {
      val profile = new FetchProfile()
      profile.add(FetchProfile.Item.FLAGS)
      profile.add(FetchProfile.Item.ENVELOPE)
      profile.add(FetchProfile.Item.CONTENT_INFO)
      profile.add(UIDFolder.FetchProfileItem.UID)
      profile.add("In-Reply-To")
      profile
    }

    system.scheduler.schedule(1 minute, 1 minute)({
      this.self ! Tick
    })

    def folderAddUids(folder: IMAPFolder, addedUids: Traversable[Long]) = {
      val folderURLName = folder.getURLName
      watchedFolders.get(folderURLName) match {
        case Some((_, uidValidity, uids, messageCountListener)) =>
          uids ++= addedUids
        case _ =>
      }
    }

    def folderRemoveUids(folder: IMAPFolder, removedUids: Traversable[Long]) = {
      val folderURLName = folder.getURLName
      watchedFolders.get(folderURLName) match {
        case Some((_, uidValidity, uids, messageCountListener)) =>
          uids --= removedUids
        case _ =>
      }
    }

    override def receive: Receive = super.receive orElse {
      case config: Config =>
        queue((Vector.empty, Vector(config)))
      case UpdatedMessages(added, folder, messages) =>
        val update = (folder, Some((added, messages)))
        queue((Vector(update), Vector.empty))
      case ConnectionClosed(folder) =>
        val update = (folder, None)
        queue((Vector(update), Vector.empty))
      case Tick =>
        if (queueIsEmpty && waitingForData) {
          watchedFolders.values.toIndexedSeq.map {
            case (folder, _, _, _) => try {
              if (!folder.isOpen) {
                queue((Vector((folder, None)), Vector.empty))
              } else {
                // hacky way to make servers fire MessageCountListener
                // TODO: use IMAPFolder:idle if possible
                folder.getMessageCount
              }
            } catch {
              case e: FolderNotFoundException =>
                logger.error(s"Cannot find folder ${folder.getFullName}, skipping.")
                unsubscribeFolder(folder)
              case e@(_: MessagingException | _: IllegalStateException) =>
                logger.error(s"Error reading folder ${folder.getFullName}, retrying later.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
            }
          }
        }
      case Update(diff) => sender() ! applyDiff(diff)
    }

    def unsubscribeFolder(folder: IMAPFolder, removeMessages: Boolean = true, closeFolder: Boolean = true): Unit = {
      val folderURLName = folder.getURLName
      watchedFolders.get(folderURLName) match {
        case Some((_, uidValidity, uids, (messageCountListener, connectionListener))) =>
          folder.removeMessageCountListener(messageCountListener)
          folder.removeConnectionListener(connectionListener)
          if (removeMessages) {
            val documents = uids.toVector.map {
              uid => documentForAction(RemovedMessage(folderURLName, uidValidity, uid))
            }
            queueDocuments(documents)
          }
          watchedFolders.remove(folderURLName)
        case _ =>
      }
      try {
        if (closeFolder && folder.isOpen) {
          folder.close(false)
        }
      } catch {
        case _: MessagingException | _: IllegalStateException =>
      }
    }

    private def documentForAction(action: ImapAction): Document = {
      val context = messageContext(action.folderURLName, action.uidValidity, action.messageUid)
      action match {
        case action: AddedMessage =>
          Document(context, emailMessageConverter.convert(action.message, context))
        case action: RemovedMessage =>
          Document(context, SimpleHashModel.empty)
      }
    }

    private def messageContext(folderURLName: URLName, uidValidity: Long, messageUid: Long): IRI = {
      valueFactory.createIRI(s"$folderURLName#$uidValidity-$messageUid")
    }

    private def waitingForData: Boolean = {
      isActive && totalDemand > 0
    }

    def createFolderListeners(folder: IMAPFolder) = {
      val messageCountListener = new MessageCountListener {
        override def messagesAdded(e: MessageCountEvent): Unit = {
          Publisher.this.self ! UpdatedMessages(added = true, folder, e.getMessages.toVector.asInstanceOf[Vector[IMAPMessage]])
        }

        override def messagesRemoved(e: MessageCountEvent): Unit = {
          Publisher.this.self ! UpdatedMessages(added = false, folder, e.getMessages.toVector.asInstanceOf[Vector[IMAPMessage]])
        }
      }
      val connectionListener = new ConnectionListener {
        override def disconnected(e: ConnectionEvent): Unit = {
        }

        override def opened(e: ConnectionEvent): Unit = {
        }

        override def closed(e: ConnectionEvent): Unit = {
          Publisher.this.self ! ConnectionClosed(folder)
        }
      }
      folder.addMessageCountListener(messageCountListener)
      folder.addConnectionListener(connectionListener)
      (messageCountListener, connectionListener)
    }

    def openFolder(folder: IMAPFolder) = {
      if (!folder.isOpen) {
        watchedFolders.get(folder.getURLName) match {
          case None =>
            folder.open(Folder.READ_ONLY)
            val folderListeners = createFolderListeners(folder)
            watchedFolders += folder.getURLName ->(folder, folder.getUIDValidity, new mutable.HashSet[Long], folderListeners)
          case Some((previousFolder, uidValidity, uids, previousFolderListeners)) =>
            folder.open(Folder.READ_ONLY)
            if (folder.getUIDValidity != uidValidity || folder != previousFolder) {
              unsubscribeFolder(previousFolder, removeMessages = folder.getUIDValidity != uidValidity, closeFolder = folder != previousFolder)
              val folderListeners = if (folder != previousFolder) {
                createFolderListeners(folder)
              } else {
                previousFolderListeners
              }
              watchedFolders += folder.getURLName ->(folder, folder.getUIDValidity, if (folder.getUIDValidity != uidValidity) new mutable.HashSet[Long] else uids, folderListeners)
            }
        }
        true
      } else {
        false
      }
    }

    def fetchFolderMessagesWithUIDs(folder: IMAPFolder) = {
      TimeExecution.timeInfo(s"Fetching Email message UIDs for folder ${folder.getFullName}", logger, {
        val messages = folder.getMessages
        folder.fetch(messages, uidFetchProfile)
        messages.toVector.asInstanceOf[Vector[IMAPMessage]]
      })
    }

    override protected def queryBuilder: ((Vector[(IMAPFolder, Option[(Boolean, Vector[IMAPMessage])])], Vector[Config]), Long) => Future[Result] = {
      case (state@(folders, queuedConfigs), demand) =>
        Future {
          folders match {
            case (currentFolder, Some((added, messages))) +: foldersTail =>
              if (added) {
                try {
                  if (openFolder(currentFolder)) {
                    logger.info(s"Email folder ${currentFolder.getFullName} needs to be reloaded.")
                    Result(Some((currentFolder, None) +: foldersTail, queuedConfigs), Vector.empty)
                  } else {
                    val count = Math.min(Math.min(fetchedMessagesMaxBufferSize, Math.min(demand * fetchedMessagesDemandMultiplier, Int.MaxValue).toInt), messages.length)
                    val (messagesToFetch, remainingMessages) = messages.splitAt(count)
                    val (uidsAndDocuments, unprocessedMessages) = getDocumentsForMessages(currentFolder, messagesToFetch)
                    val documents = uidsAndDocuments.map(_._2)
                    folderAddUids(currentFolder, uidsAndDocuments.map(_._1))
                    if (unprocessedMessages.isEmpty && remainingMessages.isEmpty) {
                      logger.info(s"Email folder ${currentFolder.getFullName} fully imported.")
                      Result(Some((foldersTail, queuedConfigs)), documents)
                    } else {
                      Result(Some(((currentFolder, Some((added, unprocessedMessages ++ remainingMessages))) +: foldersTail, queuedConfigs)), documents)
                    }
                  }
                } catch {
                  case e: FolderNotFoundException =>
                    logger.error(s"Cannot find folder ${currentFolder.getFullName}, skipping.")
                    unsubscribeFolder(currentFolder)
                    Result(Some((foldersTail, queuedConfigs)), Vector.empty)
                  case e@(_: MessagingException | _: IllegalStateException) =>
                    logger.error(s"Error reading messages from folder ${currentFolder.getFullName}, retrying.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
                    try {
                      if (currentFolder.isOpen) {
                        currentFolder.close(false)
                      }
                    } catch {
                      case _: MessagingException | _: IllegalStateException =>
                    }
                    Thread.sleep(retryTime.toMillis)
                    Result(Some(state), Vector.empty)
                }
              } else {
                try {
                  if (currentFolder.isOpen) {
                    val uidValidity = currentFolder.getUIDValidity
                    val uidsAndDocuments = messages.flatMap(message => {
                      try {
                        // message UID is retrieved from the cache, only if the message had been fetched already.
                        val messageUid = currentFolder.getUID(message)
                        Some((messageUid, documentForAction(RemovedMessage(currentFolder.getURLName, uidValidity = uidValidity, messageUid = messageUid))))
                      } catch {
                        case e: MessageRemovedException =>
                          logger.warn(s"Message $message was removed before it had been fetched, skipping.")
                          None
                      }
                    })
                    folderRemoveUids(currentFolder, uidsAndDocuments.map(_._1))
                    Result(Some((foldersTail, queuedConfigs)), uidsAndDocuments.map(_._2))
                  } else {
                    // if folder is not open, we cannot remove messages, since the UIDs have not been loaded.
                    Result(Some((foldersTail, queuedConfigs)), Vector.empty)
                  }
                } catch {
                  case e@(_: MessagingException | _: IllegalStateException) =>
                    logger.error(s"Error deleting messages from folder ${currentFolder.getFullName}, skipping.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
                    Result(Some((foldersTail, queuedConfigs)), Vector.empty)
                }
              }
            case (currentFolder, None) +: foldersTail =>
              try {
                openFolder(currentFolder)
                val messages = fetchFolderMessagesWithUIDs(currentFolder)
                val folderURLName = currentFolder.getURLName
                val (_, uidValidity, messageUidsAlreadyAdded, _) = watchedFolders(folderURLName)
                val messagesToAdd = if (messageUidsAlreadyAdded.nonEmpty) {
                  // some Messages have already been inserted, we must filter some additions and do some removals in order to sync the KB.
                  val messagesWithUids = messages.map {
                    case (message) => (currentFolder.getUID(message), message)
                  }
                  val messageUidsToAdd = messagesWithUids.map(_._1).toSet
                  val uidsToRemove = messageUidsAlreadyAdded -- messageUidsToAdd
                  val removedDocuments = uidsToRemove.toVector.map {
                    uid => documentForAction(RemovedMessage(folderURLName, uidValidity, uid))
                  }
                  folderRemoveUids(currentFolder, uidsToRemove)
                  if (removedDocuments.nonEmpty) {
                    logger.info(s"Resuming import of email folder ${currentFolder.getFullName}: ${removedDocuments.length} messages to remove.")
                  }
                  queueDocuments(removedDocuments)
                  messagesWithUids.collect {
                    case (uid, message) if !messageUidsAlreadyAdded.contains(uid) => message
                  }
                } else {
                  messages
                }
                if (messagesToAdd.nonEmpty) {
                  logger.info(s"Resuming import of email folder ${currentFolder.getFullName}: ${messagesToAdd.length} messages to add.")
                }
                Result(Some(((currentFolder, Some((true, messagesToAdd))) +: foldersTail, queuedConfigs)), Vector.empty)
              } catch {
                case e: FolderNotFoundException =>
                  logger.error(s"Cannot find folder ${currentFolder.getFullName}, skipping.")
                  unsubscribeFolder(currentFolder)
                  Result(Some((foldersTail, queuedConfigs)), Vector.empty)
                case e@(_: MessagingException | _: IllegalStateException) =>
                  logger.error(s"Error processing folder ${currentFolder.getFullName}, retrying.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
                  Thread.sleep(retryTime.toMillis)
                  Result(Some(state), Vector.empty)
              }
            case _ =>
              queuedConfigs match {
                case headConfig +: tailConfigs =>
                  try {
                    if (!headConfig.store.isConnected) {
                      headConfig.store.connect()
                    }
                    val folders = getStoreFolders(headConfig.store, headConfig.folderNamesToKeep)
                    Result(Some(folders.map((_, None)), tailConfigs), Vector.empty)
                  } catch {
                    case e: AuthenticationFailedException =>
                      logger.error(s"Invalid authentication for ${headConfig.store}, skipping.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
                      Result(Some(Vector.empty, tailConfigs), Vector.empty)
                    case e@(_: MessagingException | _: IllegalStateException) =>
                      logger.error(s"Error reading folders from ${headConfig.store}, skipping.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
                      try {
                        headConfig.store.close()
                      } catch {
                        case (_: MessagingException | _: IllegalStateException) =>
                      }
                      Result(Some(Vector.empty, tailConfigs), Vector.empty)
                  }
                case _ =>
                  Result(None, Vector.empty)
              }
          }
        }
    }

    private def getStoreFolders(store: Store, folderNamesToKeep: Option[Set[String]]): Vector[IMAPFolder] = {
      //TODO: import all folders?
      val keepFolder = folderNamesToKeep.map(x => x.contains(_: String)).getOrElse((_: String) => true)
      store.getDefaultFolder.list().collect {
        case imapFolder: IMAPFolder if folderHoldsInterestingMessages(imapFolder) && keepFolder(imapFolder.getName) => imapFolder
      }.toVector
    }

    private def folderHoldsInterestingMessages(folder: Folder): Boolean = {
      folderHoldsMessages(folder) && !ignoredFolderNames.contains(folder.getName)
    }

    private def folderHoldsMessages(folder: Folder): Boolean = {
      (folder.getType & javax.mail.Folder.HOLDS_MESSAGES) != 0
    }

    private def getDocumentsForMessages(folder: IMAPFolder, messages: Vector[IMAPMessage]) = {
      folder.fetch(messages.toArray, fetchProfile)
      var unrecoverableError = false
      val documentBuilder = Vector.newBuilder[(Long, Document)]
      val remainingMessages = Vector.newBuilder[IMAPMessage]
      messages.foreach {
        message =>
          if (unrecoverableError) {
            remainingMessages += message
          } else {
            try {
              val messageUid = folder.getUID(message)
              documentBuilder += ((messageUid, documentForAction(AddedMessage(uidValidity = folder.getUIDValidity, messageUid = messageUid, message))))
              message.invalidateHeaders()
            } catch {
              case e: FolderClosedException =>
                unrecoverableError = true
                remainingMessages += message
              case e: MessagingException =>
                logger.error(s"Error processing $message, skipping.\n${ExceptionUtils.getUnrolledStackTrace(e)}")
            }
          }
      }
      (documentBuilder.result(), remainingMessages.result())
    }

    private def applyDiff(diff: ModelDiff): UpdateResults = {
      //TODO: support at least email deletion
      //We tag only the messages from IMAP as failed
      UpdateResults.merge(
        diff.contexts().asScala
          .filter(_.stringValue().startsWith("imap://"))
          .map(context => UpdateResults.allFailed(
            diff.filter(null, null, null, context),
            new ConverterException("IMAP repository could not be modified")
          ))
      )
    }
  }

  object Tick
}
