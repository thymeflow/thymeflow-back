package com.thymeflow.rdf.model.document

import com.thymeflow.rdf.model.StatementSet
import org.eclipse.rdf4j.model.IRI

/**
  * @author Thomas Pellissier Tanon
  */
case class Document(iri: IRI, statements: StatementSet){
  require(statements.forall(s => s.getContext != null), s"All statements within a document must have a context. Found ${statements.find(_.getContext == null)} instead.")
}
