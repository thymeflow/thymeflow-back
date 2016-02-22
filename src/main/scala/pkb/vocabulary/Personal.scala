package pkb.vocabulary

import org.openrdf.model.IRI
import org.openrdf.model.impl.SimpleValueFactory

object Personal {
  val NAMESPACE: String = "http://thomas.pellissier-tanon.fr/personal#"

  val EMAIL_ADDRESS: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "EmailAddress")
  val PHONE_NUMBER: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "PhoneNumber")
  val PRIMARY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "primaryRecipient")
  val COPY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "copyRecipient")
  val BLIND_COPY_RECIPIENT: IRI = SimpleValueFactory.getInstance().createIRI(NAMESPACE, "blindCopyRecipient")
}
