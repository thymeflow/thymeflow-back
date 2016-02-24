package pkb.sync

import javax.mail.{Folder, Store}

import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.{Model, ValueFactory}
import pkb.sync.converter.EmailMessageConverter

/**
  * @author Thomas Pellissier Tanon
  */
class EmailSynchronizer(valueFactory: ValueFactory) {

  private val emailMessageConverter = new EmailMessageConverter(valueFactory)

  def synchronize(store: Store, limit: Int): Model = {
    val model = new LinkedHashModel
    addFolderToModel(store.getFolder("INBOX"), limit, model) //TODO: discover other folders
    model
  }

  private def addFolderToModel(folder: Folder, limit: Int, model: Model): Unit = {
    folder.open(Folder.READ_ONLY)
    model.addAll(emailMessageConverter.convert(folder.getMessages.take(limit)))
  }
}
