package com.thymeflow.rdf.model.document

import com.thymeflow.rdf.model.StatementSet
import org.eclipse.rdf4j.model.IRI

/**
  * @author Thomas Pellissier Tanon
  */
case class Document(iri: IRI, statements: StatementSet)
