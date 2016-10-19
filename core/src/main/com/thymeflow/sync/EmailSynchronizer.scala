package com.thymeflow.sync

import java.time.Instant
import javax.mail.event._
import javax.mail.{FolderClosedException, _}

import akka.actor.{ActorRef, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import com.sun.mail.imap.{IMAPFolder, IMAPMessage}
import com.thymeflow.rdf.model.document.Document
import com.thymeflow.rdf.model.{ModelDiff, SimpleHashModel}
import com.thymeflow.service.source.ImapSource
import com.thymeflow.service.{Progress, ServiceAccountSource, ServiceAccountSourceTask, ServiceAccountSources, TaskStatus, Done => DoneService, Idle => IdleService, Working => WorkingService}
import com.thymeflow.sync.Synchronizer.Update
import com.thymeflow.sync.converter.{ConverterException, EmailMessageConverter}
import com.thymeflow.update.UpdateResults
import com.thymeflow.utilities.TimeExecution
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.openrdf.model.{IRI, ValueFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.concurrent.Future
import scala.language.postfixOps

/**
  * @author Thomas Pellissier Tanon
  * @author David Montoya
  */
object EmailSynchronizer extends Synchronizer with StrictLogging {

  implicit val executionContext = com.thymeflow.actors.executor
  // Prevent memory leaks due to caching of Messages' multipart content.
  System.setProperty("mail.mime.cachemultipart", "false")

  private final val fetchedMessagesMaxBufferSize = 512
  private final val fetchedMessagesDemandMultiplier = 32L

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

  private val ignoredFolderNames = Set("Junk", "Deleted", "Deleted Messages", "Spam")

  def source(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config) =
    Source.actorPublisher[Document](Props(new Publisher(valueFactory, supervisor)))

  private sealed trait ImapAction {
    def uidValidity: Long

    def messageUid: Long

    def folderURLName: URLName
  }

  private case class AddedMessage(uidValidity: Long, messageUid: Long, message: Message) extends ImapAction {
    def folderURLName = message.getFolder.getURLName
  }

  private case class RemovedMessage(folderURLName: URLName, uidValidity: Long, messageUid: Long) extends ImapAction

  private sealed trait QueuedMessage

  private case class UpdatedSource(fetcher: EmailFetcher, source: ImapSource) extends QueuedMessage

  private case class UpdatedMessages(fetcher: EmailFetcher, added: Boolean, folder: IMAPFolder, messages: Vector[IMAPMessage]) extends QueuedMessage

  private case class ConnectionClosed(fetcher: EmailFetcher, folder: IMAPFolder) extends QueuedMessage

  private case class FolderSyncStatus(uidValidity: Long,
                                      addedMessageUids: Set[Long],
                                      messagesToAdd: Vector[IMAPMessage],
                                      messagesToRemove: Vector[IMAPMessage])

  private case class FolderSubscription(folder: IMAPFolder, messageCountListener: MessageCountListener, connectionListener: ConnectionListener, inSync: Boolean)

  private case class ConnectedSource(task: ServiceAccountSourceTask[TaskStatus], source: ImapSource)

  private object Tick

  private case class Documents(content: Traversable[Document])

  private case class Result(states: Traversable[(EmailFetcher, (EmailFetcherState, EmailFetcherTask))])

  private sealed trait EmailFetcherTask

  private case object Idle extends EmailFetcherTask

  private case class Working(folderURLName: URLName, startDate: Instant, processed: Long, toProcess: Long) extends EmailFetcherTask

  private case class Done(folderURLName: URLName, startDate: Instant, endDate: Instant) extends EmailFetcherTask

  private sealed trait EmailFetcherState

  private case class Initial(source: ImapSource) extends EmailFetcherState

  private sealed trait FoldersEmailFetcherState extends EmailFetcherState

  private case class Connected(source: ImapSource,
                               folders: Map[URLName, (FolderSubscription, Option[FolderSyncStatus])]) extends FoldersEmailFetcherState

  private case class Error(source: ImapSource,
                           foldersOption: Option[Map[URLName, (FolderSubscription, Option[FolderSyncStatus])]]) extends FoldersEmailFetcherState

  private case class NewSource(source: ImapSource, state: FoldersEmailFetcherState) extends EmailFetcherState

  private class Publisher(valueFactory: ValueFactory, supervisor: ActorRef)(implicit config: Config)
    extends ActorPublisher[Document] {

    private val fetchers = new mutable.HashMap[ServiceAccountSource, (EmailFetcher, EmailFetcherState, EmailFetcherTask)]

    protected def newFetcher(source: ImapSource, serviceSource: ServiceAccountSource) = {
      new EmailFetcher(self, valueFactory, serviceSource, source)
    }

    def notifyTask(fetcher: EmailFetcher, fetcherTask: EmailFetcherTask) = {
      val serviceTask = fetcherTask match {
        case Idle =>
          ServiceAccountSourceTask(fetcher.serviceAccountSource, "Synchronization", IdleService)
        case Working(folderURLName, startDate, processed, toProcess) =>
          ServiceAccountSourceTask(fetcher.serviceAccountSource, Some(folderURLName.getFile).getOrElse(""), WorkingService(startDate, Some(Progress(value = processed, total = toProcess))))
        case Done(folderURLName, startDate, endDate) =>
          ServiceAccountSourceTask(fetcher.serviceAccountSource, Some(folderURLName.getFile).getOrElse(""), DoneService(startDate, endDate))
      }
      supervisor ! serviceTask
    }

    protected def addOrUpdateFetcher(sourceId: ServiceAccountSource, source: ImapSource): Unit = {
      // If there exists a fetcher for his sourceId we only update its source
      fetchers.get(sourceId) match {
        case Some((fetcher, _, _)) =>
          messageQueue :+= UpdatedSource(fetcher, source)
        case None =>
          fetchers(sourceId) = {
            val fetcher = newFetcher(source, sourceId)
            val fetcherState = Initial(source)
            val fetcherTask = Idle
            notifyTask(fetcher, fetcherTask)
            (fetcher, fetcherState, fetcherTask)
          }
      }
    }

    private var processing = false
    private var buf = Vector.empty[Document]

    @tailrec final protected def deliverBuf(delivered: Long = 0): Long =
      if (isActive && totalDemand > 0) {
        /*
         * totalDemand is a Long and could be larger than
         * what buf.splitAt can accept
         */
        if (totalDemand <= Int.MaxValue) {
          val (use, keep) = buf.splitAt(totalDemand.toInt)
          buf = keep
          use foreach onNext
          use.size
        } else {
          val (use, keep) = buf.splitAt(Int.MaxValue)
          buf = keep
          use foreach onNext
          deliverBuf(delivered + use.size)
        }
      } else {
        0L
      }


    def handleState(handlers: Traversable[(EmailFetcher, () => (EmailFetcherState, EmailFetcherTask))]) = {
      processing = true
      val future = Future {
        handlers.map {
          case (fetcher, handler) => (fetcher, handler())
        }
      }
      future.foreach {
        result => self ! Result(result)
      }
      future.onFailure {
        case t => logger.error(s"Error handling state.", t)
      }
    }

    def shouldFetch = !processing && isActive && totalDemand > 0

    def handleFetchersState() = {
      if (shouldFetch) {
        processMessageQueue()
        fetchers.values.view.map {
          case (fetcher, fetcherState, fetcherTask) => {
            (fetcher, fetcher.stateHandler(fetcherState, fetcherTask, totalDemand))
          }
        }.collectFirst {
          case (fetcher, Some(handler)) => (fetcher, handler)
        }.exists {
          case (fetcher, handler) =>
            handleState(List((fetcher, handler)))
            true
        }
      } else {
        false
      }
    }

    var messageQueue = Vector.empty[QueuedMessage]

    def processMessageQueue() = {
      messageQueue.foreach {
        case ConnectionClosed(fetcher, folder) =>
          fetchers.get(fetcher.serviceAccountSource) match {
            case Some((_, connected@Connected(_, folders), task)) =>
              val newFolders = folders.get(folder.getURLName) match {
                case Some((folderSubscription@FolderSubscription(_, _, _, true), syncStatusOption)) =>
                  folders + (folder.getURLName -> (folderSubscription.copy(inSync = false), syncStatusOption))
                case _ => folders
              }
              fetchers += fetcher.serviceAccountSource -> (fetcher, connected.copy(folders = newFolders), task)
            case _ =>
          }
        case UpdatedSource(fetcher, newSource) =>
          fetchers.get(fetcher.serviceAccountSource) match {
            case Some((_, fetcherState, task)) =>
              val newState = fetcherState match {
                case state: FoldersEmailFetcherState => NewSource(newSource, state)
                case NewSource(_, state) => NewSource(newSource, state)
                case Initial(_) => Initial(newSource)
              }
              fetchers += fetcher.serviceAccountSource -> (fetcher, newState, task)
            case _ =>
          }
        case UpdatedMessages(fetcher, added, folder, messages) =>
          fetchers.get(fetcher.serviceAccountSource) match {
            case Some((_, fetcherState, task)) =>
              val newState = fetcherState match {
                case connected@Connected(_, folders) =>
                  val newFolders = folders.get(folder.getURLName) match {
                    case Some((folderSubscription@FolderSubscription(_, _, _, true), Some(syncStatus))) =>
                      val newSyncStatus = if (added) {
                        syncStatus.copy(messagesToAdd = syncStatus.messagesToAdd ++ messages)
                      } else {
                        syncStatus.copy(messagesToRemove = syncStatus.messagesToRemove ++ messages)
                      }
                      folders + (folder.getURLName -> (folderSubscription, Some(newSyncStatus)))
                    case _ => folders
                  }
                  fetchers += fetcher.serviceAccountSource -> (fetcher, connected.copy(folders = newFolders), task)
                case _ =>
              }
            case _ =>
          }
      }
      messageQueue = Vector.empty
    }

    def receive: Receive = {
      case serviceAccountSources: ServiceAccountSources =>
        serviceAccountSources.sources.foreach {
          case (serviceAccountSource, source: ImapSource) =>
            addOrUpdateFetcher(serviceAccountSource, source)
          case _ =>
        }
        handleFetchersState()
      case Result(states) =>
        processing = false
        states.foreach {
          case (fetcher, (state, task)) =>
            fetchers += fetcher.serviceAccountSource -> (fetcher, state, task)
            notifyTask(fetcher, task)
        }
        handleFetchersState()
      case Documents(content) =>
        buf ++= content
        deliverBuf()
      case Request(_) =>
        deliverBuf()
        handleFetchersState()
      case Cancel =>
        context.stop(self)
      case Tick =>
        if (shouldFetch) {
          if (!handleFetchersState()) {
            val handlers = fetchers.values.toVector.map {
              case (fetcher, state, task) =>
                (fetcher, () => {
                  fetcher.onTick(state, task)
                })
            }
            handleState(handlers)
          }
        }
      case Update(diff) => sender() ! applyDiff(diff)
      case m: QueuedMessage =>
        messageQueue :+= m
        handleFetchersState()
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


  private class EmailFetcher(publisher: ActorRef,
                             valueFactory: ValueFactory,
                             val serviceAccountSource: ServiceAccountSource,
                             initialSource: ImapSource)
                            (implicit config: Config) {

    protected def publishDocuments(documents: Traversable[Document]) = {
      publisher ! Documents(documents)
    }

    def onTick(state: EmailFetcherState, task: EmailFetcherTask) = {
      // state is assumed to be Connected, and folder subscription is assumed to in sync
      state match {
        case connected@Connected(source, folders) =>
          (folders.values.foldLeft(connected) {
            case (foldedConnected, (folderSubscription@FolderSubscription(folder, _, _, true), syncStatus)) =>
              try {
                scala.concurrent.blocking {
                  if (!folder.isOpen) {
                    // hacky way to make servers fire MessageCountListener
                    // TODO: use IMAPFolder:idle if possible
                    folder.getMessageCount
                  }
                }
                foldedConnected
              } catch {
                case e: FolderNotFoundException =>
                  logger.error(s"$serviceAccountSource: Cannot find folder ${folder.getFullName}, skipping.")
                  unsubscribeFolder(folderSubscription, close = true)
                  foldedConnected.copy(folders = folders - folder.getURLName)
                case e@(_: MessagingException | _: IllegalStateException) =>
                  logger.error(s"$serviceAccountSource: Error reading folder ${folder.getFullName}, retrying later.", e)
                  foldedConnected.copy(folders = folders + (folder.getURLName -> (folderSubscription.copy(inSync = false), syncStatus)))
              }
          }, task)
        case _ =>
          (state, task)
      }
    }

    def stateHandler(state: EmailFetcherState, task: EmailFetcherTask, demand: Long): Option[() => (EmailFetcherState, EmailFetcherTask)] = {
      state match {
        case NewSource(newSource, Error(source, folders)) => None
        case NewSource(newSource, Connected(_, folders)) =>
          Some(() => {
            folders.values.foreach {
              case (folderSubscription, _) =>
                unsubscribeFolder(folderSubscription, close = true)
              case _ =>
            }
            try {
              val store = newSource.connect()
              val newFolders = getStoreFolders(store, newSource.folderNamesToKeep)
              val newFolderURLNameSet = newFolders.map(_.getURLName).toSet
              val removedFolders = folders.keySet -- newFolderURLNameSet
              removedFolders.foreach {
                removedFolderURLName =>
                  folders(removedFolderURLName) match {
                    case (_, Some(folderSyncStatus)) =>
                      removeMessagesFromFolder(removedFolderURLName, folderSyncStatus)
                    case _ =>
                  }
              }
              val folderStatus = newFolders.map {
                folder =>
                  val folderSyncStatus = folders.get(folder.getURLName) match {
                    case Some((_, previousFolderSyncStatusOption)) =>
                      previousFolderSyncStatusOption.map {
                        previousFolderSyncStatus => previousFolderSyncStatus.copy(messagesToAdd = Vector.empty, messagesToRemove = Vector.empty)
                      }
                    case None =>
                      None
                  }
                  val folderSubscription = subscribeFolder(folder)
                  folder.getURLName -> (folderSubscription, folderSyncStatus)
              }
              (Connected(newSource, folderStatus.toMap), Idle)
            } catch {
              // TODO : make sure store is closed.
              case e: AuthenticationFailedException =>
                logger.error(s"$serviceAccountSource: Invalid authentication.", e)
                (Error(newSource, Some(folders)), Idle)
              case e@(_: MessagingException | _: IllegalStateException) =>
                logger.error(s"$serviceAccountSource: Error reading folders.", e)
                (Error(newSource, Some(folders)), Idle)
            }
          })
        case Error(source, folders) => None
        case Initial(source) =>
          Some(() =>
            try {
              val store = source.connect()
              val folders = getStoreFolders(store, source.folderNamesToKeep).map {
                folder => folder.getURLName -> (subscribeFolder(folder), None)
              }
              (Connected(source, folders.toMap), Idle)
            } catch {
              // TODO : make sure store is closed.
              case e: AuthenticationFailedException =>
                logger.error(s"$serviceAccountSource: Invalid authentication for $serviceAccountSource, skipping.", e)
                (Error(source, None), Idle)
              case e@(_: MessagingException | _: IllegalStateException) =>
                logger.error(s"$serviceAccountSource: Error reading folders from $serviceAccountSource, skipping.", e)
                (Error(source, None), Idle)
            })
        case connected@Connected(source, folders) =>
          folders.values.collectFirst {
            case (subscribedFolder@FolderSubscription(folder, _, _, true), Some(syncStatus)) if syncStatus.messagesToAdd.nonEmpty || syncStatus.messagesToRemove.nonEmpty =>
              () =>
                try {
                  if (openFolder(folder)) {
                    logger.info(s"$serviceAccountSource: Email folder ${folder.getFullName} needs to be reloaded.")
                    (connected.copy(folders = folders + (folder.getURLName -> (subscribedFolder.copy(inSync = false), Some(syncStatus)))), Idle)
                  } else {
                    var justProcessed = 0L
                    val syncStatusAfterRemove = if (syncStatus.messagesToRemove.nonEmpty) {
                      val uidValidity = folder.getUIDValidity
                      val uidsAndDocuments = syncStatus.messagesToRemove.flatMap(message => {
                        try {
                          // message UID is retrieved from the cache, only if the message had been fetched already.
                          val messageUid = folder.getUID(message)
                          Some((messageUid, documentForAction(RemovedMessage(folder.getURLName, uidValidity = uidValidity, messageUid = messageUid))))
                        } catch {
                          case e: MessageRemovedException =>
                            logger.warn(s"$serviceAccountSource: Message $message was removed before it had been fetched, skipping.")
                            None
                        }
                      })
                      val documentsToRemove = uidsAndDocuments.filter(x => syncStatus.addedMessageUids.contains(x._1))
                      justProcessed += syncStatus.messagesToRemove.size
                      publishDocuments(documentsToRemove.map(_._2))
                      syncStatus.copy(addedMessageUids = syncStatus.addedMessageUids -- documentsToRemove.map(_._1))
                    } else {
                      syncStatus
                    }
                    val syncStatusAfterAdd = if (syncStatusAfterRemove.messagesToAdd.nonEmpty) {
                      val count = Math.min(Math.min(fetchedMessagesMaxBufferSize, Math.min(demand * fetchedMessagesDemandMultiplier, Int.MaxValue).toInt), syncStatusAfterRemove.messagesToAdd.length)
                      val (messagesToFetch, remainingMessages) = syncStatusAfterRemove.messagesToAdd.splitAt(count)
                      val (uidsAndDocuments, unprocessedMessages) = getDocumentsForMessages(folder, messagesToFetch)
                      val documents = uidsAndDocuments.map(_._2)
                      val addedMessageUids = syncStatusAfterRemove.addedMessageUids ++ uidsAndDocuments.map(_._1)
                      justProcessed += messagesToFetch.size - unprocessedMessages.size
                      publishDocuments(documents)
                      syncStatusAfterRemove.copy(addedMessageUids = addedMessageUids, messagesToAdd = remainingMessages ++ unprocessedMessages)
                    } else {
                      syncStatusAfterRemove
                    }
                    val newTask = task match {
                      case working@Working(folderURLName, _, processed, _) if folderURLName == folder.getURLName =>
                        folder.getURLName
                        working.copy(processed = processed + justProcessed)
                      case _ => Working(folderURLName = folder.getURLName,
                        startDate = Instant.now,
                        processed = 0L,
                        toProcess = syncStatusAfterAdd.messagesToAdd.length.toLong + syncStatusAfterAdd.messagesToRemove.length.toLong)
                    }
                    val finalTask = if (syncStatusAfterAdd.messagesToAdd.isEmpty && syncStatusAfterAdd.messagesToRemove.isEmpty) {
                      logger.info(s"$serviceAccountSource: Email folder ${folder.getFullName} fully synchronized.")
                      Done(folderURLName = newTask.folderURLName, startDate = newTask.startDate, endDate = Instant.now())
                    } else {
                      newTask
                    }
                    (connected.copy(
                      folders = folders + (folder.getURLName -> (subscribedFolder, Some(syncStatusAfterAdd)))
                    ), finalTask)
                  }
                } catch {
                  case e: FolderNotFoundException =>
                    logger.error(s"$serviceAccountSource: Cannot find folder ${folder.getFullName}, skipping.")
                    unsubscribeFolder(subscribedFolder)
                    removeMessagesFromFolder(folder.getURLName, syncStatus)
                    (connected.copy(folders = folders - folder.getURLName), Idle)
                  case e@(_: MessagingException | _: IllegalStateException) =>
                    logger.error(s"$serviceAccountSource: Error reading messages from folder ${folder.getFullName}, retrying.", e)
                    try {
                      if (folder.isOpen) {
                        folder.close(false)
                      }
                    } catch {
                      case _: MessagingException | _: IllegalStateException =>
                    }
                    (connected.copy(folders = folders + (folder.getURLName -> (subscribedFolder.copy(inSync = false), Some(syncStatus)))), Idle)
                }
            case (folderSubscription@FolderSubscription(folder, _, _, false), previousFolderSyncStatusOption) =>
              () =>
                try {
                  openFolder(folder)
                  try {
                    val initialFolderSyncStatus = previousFolderSyncStatusOption match {
                      case Some(previousFolderSyncStatus) =>
                        if (folder.getUIDValidity != previousFolderSyncStatus.uidValidity) {
                          removeMessagesFromFolder(folder.getURLName, previousFolderSyncStatus)
                          FolderSyncStatus(folder.getUIDValidity, new immutable.HashSet[Long], Vector.empty, Vector.empty)
                        } else {
                          previousFolderSyncStatus.copy(messagesToAdd = Vector.empty, messagesToRemove = Vector.empty)
                        }
                      case None =>
                        FolderSyncStatus(folder.getUIDValidity, new immutable.HashSet[Long], Vector.empty, Vector.empty)
                    }
                    syncFolder(previousFolderSyncStatusOption.isEmpty, folder, initialFolderSyncStatus) match {
                      case Left(folderSyncStatus) =>
                        (connected.copy(folders = folders + (folder.getURLName -> (folderSubscription.copy(inSync = true), Some(folderSyncStatus)))),
                          Working(folderURLName = folder.getURLName,
                            startDate = Instant.now(),
                            processed = 0L,
                            toProcess = folderSyncStatus.messagesToAdd.length.toLong + folderSyncStatus.messagesToRemove.length.toLong))
                      case Right(true) =>
                        unsubscribeFolder(folderSubscription)
                        (connected.copy(folders = folders - folder.getURLName), Idle)
                      case Right(false) =>
                        unsubscribeFolder(folderSubscription)
                        (connected.copy(folders = folders + (folder.getURLName -> (folderSubscription.copy(inSync = false), Some(initialFolderSyncStatus)))), Idle)
                    }
                  } catch {
                    case e@(_: MessagingException | _: IllegalStateException) =>
                      logger.error(s"$serviceAccountSource: Error processing folder ${folder.getFullName}, retrying.", e)
                      unsubscribeFolder(folderSubscription)
                      (connected, Idle)
                  }
                } catch {
                  case e: FolderNotFoundException =>
                    logger.error(s"$serviceAccountSource: Cannot find folder ${folder.getFullName}, skipping.")
                    previousFolderSyncStatusOption.foreach {
                      previousFolderSyncStatus => removeMessagesFromFolder(folder.getURLName, previousFolderSyncStatus)
                    }
                    (connected.copy(folders = folders - folder.getURLName), Idle)
                  case e@(_: MessagingException | _: IllegalStateException) =>
                    logger.error(s"$serviceAccountSource: Error processing folder ${folder.getFullName}, retrying.", e)
                    (connected, Idle)
                }
          }
      }
    }

    private val emailMessageConverter = new EmailMessageConverter(valueFactory)

    def createFolderListeners(folder: IMAPFolder) = {
      val messageCountListener = new MessageCountListener {
        override def messagesAdded(e: MessageCountEvent): Unit = {
          publisher ! UpdatedMessages(EmailFetcher.this, added = true, folder, e.getMessages.toVector.asInstanceOf[Vector[IMAPMessage]])
        }

        override def messagesRemoved(e: MessageCountEvent): Unit = {
          publisher ! UpdatedMessages(EmailFetcher.this, added = false, folder, e.getMessages.toVector.asInstanceOf[Vector[IMAPMessage]])
        }
      }
      val connectionListener = new ConnectionListener {
        override def disconnected(e: ConnectionEvent): Unit = {
        }

        override def opened(e: ConnectionEvent): Unit = {
        }

        override def closed(e: ConnectionEvent): Unit = {
          publisher ! ConnectionClosed(EmailFetcher.this, folder)
        }
      }
      folder.addMessageCountListener(messageCountListener)
      folder.addConnectionListener(connectionListener)
      (messageCountListener, connectionListener)
    }


    def subscribeFolder(folder: IMAPFolder) = {
      val (messageCountListener, connectionListener) = createFolderListeners(folder)
      FolderSubscription(folder, messageCountListener, connectionListener, inSync = false)
    }

    def openFolder(folder: IMAPFolder) = {
      if (!folder.isOpen) {
        scala.concurrent.blocking {
          folder.open(Folder.READ_ONLY)
        }
        true
      } else {
        false
      }
    }

    def removeMessagesFromFolder(folderURLName: URLName,
                                 folderSyncStatus: FolderSyncStatus) = {
      val documents = folderSyncStatus.addedMessageUids.toVector.map {
        uid => documentForAction(RemovedMessage(folderURLName, folderSyncStatus.uidValidity, uid))
      }
      publishDocuments(documents)
    }

    def unsubscribeFolder(folderSubscription: FolderSubscription,
                          close: Boolean = true): Unit = {
      folderSubscription.folder.removeMessageCountListener(folderSubscription.messageCountListener)
      folderSubscription.folder.removeConnectionListener(folderSubscription.connectionListener)
      try {
        if (close && folderSubscription.folder.isOpen) {
          folderSubscription.folder.close(false)
        }
      } catch {
        case _: MessagingException | _: IllegalStateException =>
      }
    }

    private def messageContext(folderURLName: URLName, uidValidity: Long, messageUid: Long): IRI = {
      valueFactory.createIRI(s"$folderURLName#$uidValidity-$messageUid")
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

    private def getStoreFolders(store: Store, folderNamesToKeep: Option[Set[String]]): Vector[IMAPFolder] = {
      //TODO: import all folders?
      val keepFolder = folderNamesToKeep.map(x => x.contains(_: String)).getOrElse((_: String) => true)
      store.getDefaultFolder.list().collect {
        case imapFolder: IMAPFolder if folderHoldsInterestingMessages(imapFolder) && keepFolder(imapFolder.getName) => imapFolder
      }.toVector
    }

    private def folderHoldsInterestingMessages(folder: IMAPFolder): Boolean = {
      folderHoldsMessages(folder) && !ignoredFolderNames.contains(folder.getName)
    }

    private def folderHoldsMessages(folder: IMAPFolder): Boolean = {
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
              case e: MessageRemovedException =>
                logger.warn(s"$serviceAccountSource: Message $message was removed, skipping.")
              case e: MessagingException =>
                logger.error(s"$serviceAccountSource: Error processing $message, skipping.", e)
            }
          }
      }
      (documentBuilder.result(), remainingMessages.result())
    }

    def syncFolder(firstSync: Boolean, currentFolder: IMAPFolder, syncStatus: FolderSyncStatus) = {
      try {
        val messages = fetchFolderMessagesWithUIDs(currentFolder)
        val folderURLName = currentFolder.getURLName
        val messageUidsAlreadyAdded = syncStatus.addedMessageUids
        val (messagesToAdd, messageUidsToRemove) = if (messageUidsAlreadyAdded.nonEmpty) {
          // some Messages have already been inserted, we must filter some additions and do some removals in order to sync the KB.
          val messagesWithUids = messages.map {
            case (message) => (currentFolder.getUID(message), message)
          }
          val messageUidsToAdd = messagesWithUids.map(_._1).toSet
          val uidsToRemove = messageUidsAlreadyAdded -- messageUidsToAdd
          val removedDocuments = uidsToRemove.toVector.map {
            uid => documentForAction(RemovedMessage(folderURLName, syncStatus.uidValidity, uid))
          }
          if (removedDocuments.nonEmpty) {
            logger.info(s"$serviceAccountSource: Resuming import of email folder ${currentFolder.getFullName}. ${removedDocuments.length} messages to remove.")
          }
          publishDocuments(removedDocuments)
          (messagesWithUids.collect {
            case (uid, message) if !messageUidsAlreadyAdded.contains(uid) => message
          }, uidsToRemove)
        } else {
          (messages, Set.empty)
        }
        if (firstSync) {
          logger.info(s"$serviceAccountSource: Starting import of email folder ${currentFolder.getFullName}: ${messagesToAdd.length} messages to add.")
        } else {
          logger.info(s"$serviceAccountSource: Resuming import of email folder ${currentFolder.getFullName}: ${messagesToAdd.length} messages to add.")
        }
        Left(syncStatus.copy(addedMessageUids = syncStatus.addedMessageUids -- messageUidsToRemove,
          messagesToAdd = messagesToAdd,
          messagesToRemove = Vector.empty))
      } catch {
        case e: FolderNotFoundException =>
          logger.error(s"$serviceAccountSource: Cannot find folder ${currentFolder.getFullName}, skipping.")
          removeMessagesFromFolder(currentFolder.getURLName, syncStatus)
          Right(true)
        case e@(_: MessagingException | _: IllegalStateException) =>
          logger.error(s"$serviceAccountSource: Error processing folder ${currentFolder.getFullName}, retrying.", e)
          Right(false)
      }
    }

    def fetchFolderMessagesWithUIDs(folder: IMAPFolder) = {
      TimeExecution.timeInfo(s"$serviceAccountSource: Fetching message UIDs for folder ${folder.getFullName}", logger, {
        scala.concurrent.blocking {
          val messages = folder.getMessages
          folder.fetch(messages, uidFetchProfile)
          messages
        }.toVector.asInstanceOf[Vector[IMAPMessage]]
      })
    }


  }
}
