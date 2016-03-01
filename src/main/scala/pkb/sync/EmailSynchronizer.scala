package pkb.sync

import javax.mail.{Folder, Store}

import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.{Model, ValueFactory}
import pkb.rdf.model.document.Document
import pkb.sync.converter.EmailMessageConverter

/**
  * @author Thomas Pellissier Tanon
  */
class EmailSynchronizer(valueFactory: ValueFactory, store: Store, limit: Int) extends Synchronizer {

  private val emailMessageConverter = new EmailMessageConverter(valueFactory)

  def synchronize(): Traversable[Document] = {
    val model = new LinkedHashModel
    addFolderToModel(store.getFolder("INBOX"), limit, model) //TODO: discover other folders
    Array(new Document(null, model))
  }

  private def addFolderToModel(folder: Folder, limit: Int, model: Model): Unit = {
    folder.open(Folder.READ_ONLY)
    model.addAll(emailMessageConverter.convert(folder.getMessages.take(limit)))
  }
}
