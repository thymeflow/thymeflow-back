package com.thymeflow.rdf.query.algebra.evaluation.function

import com.thymeflow.rdf.model.vocabulary.Personal
import org.openrdf.model.datatypes.XMLDatatypeUtil
import org.openrdf.model.{Literal, Value, ValueFactory}
import org.openrdf.query.algebra.evaluation.{ValueExprEvaluationException, function}

/**
  * @author Thomas Pellissier Tanon
  */
class DurationInMillis extends function.Function {

  override def getURI: String = Personal.DURATION_IN_MILLIS.toString

  override def evaluate(valueFactory: ValueFactory, args: Value*): Value = {
    if(args.size != 2) {
      throw new ValueExprEvaluationException("personal:duration requires exactly 2 argument, got " + args.size)
    }

    args(0) match {
      case startLiteral: Literal if XMLDatatypeUtil.isCalendarDatatype(startLiteral.getDatatype) =>
        args(1) match {
          case endLiteral: Literal if XMLDatatypeUtil.isCalendarDatatype(endLiteral.getDatatype) =>
            valueFactory.createLiteral(
              endLiteral.calendarValue().toGregorianCalendar().getTimeInMillis - startLiteral.calendarValue().toGregorianCalendar().getTimeInMillis
            )
          case _ => throw new ValueExprEvaluationException("personal:duration requires as second argument a date literal")
        }
      case _ => throw new ValueExprEvaluationException("personal:duration requires as first argument a date literal")
    }
  }
}
