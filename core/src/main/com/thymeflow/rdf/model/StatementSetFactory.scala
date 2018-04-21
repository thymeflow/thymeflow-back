package com.thymeflow.rdf.model

import org.eclipse.rdf4j.model.{Statement, ValueFactory}

import scala.collection.generic.CanBuildFrom
import scala.collection.{GenSet, mutable}
import scala.language.higherKinds

/**
  * @author David Montoya
  */
trait StatementSetFactory[T <: StatementSet] {
  def empty(implicit valueFactory: ValueFactory): T

  def apply(statements: Statement*)(implicit valueFactory: ValueFactory): T = {
    val statementSet = empty
    statementSet ++= statements
  }

  def newBuilder(implicit valueFactory: ValueFactory): mutable.Builder[Statement, T] =
    new mutable.GrowingBuilder[Statement, T](empty)

  implicit def canBuildFrom[CC[X] <: GenSet[X]](implicit valueFactory: ValueFactory): CanBuildFrom[CC[_], Statement, T] =
    new CanBuildFrom[CC[_], Statement, T] {
      def apply(from: CC[_]): mutable.Builder[Statement, T] = newBuilder

      def apply(): mutable.Builder[Statement, T] = newBuilder
    }
}
