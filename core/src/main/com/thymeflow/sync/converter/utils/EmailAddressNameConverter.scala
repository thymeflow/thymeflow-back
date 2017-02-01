package com.thymeflow.sync.converter.utils

import java.util.Locale

import com.thymeflow.utilities.text.Normalization
import org.apache.commons.lang3.StringUtils
import org.eclipse.rdf4j.model.{Literal, ValueFactory}

import scala.util.matching.Regex

/**
  * @author David Montoya
  */
class EmailAddressNameConverter(valueFactory: ValueFactory) {

  private val emailNameFilterThreshold: Double = 0.9

  def convert(rawName: String, localPart: String, domain: String): Option[Literal] = {
    convert(rawName, EmailAddressConverter.concatenateLocalPartAndDomain(localPart, domain))
  }

  def convert(rawName: String, email: String): Option[Literal] = {
    // remove leading and trailing quotes if the name is enclosed by them.
    Some(Normalization.trimQuotes(rawName)).map(name =>
        // Filter common source of imprecision, email addresses contained within names,
        // between parenthesis, brackets or angular brackets. Case insensitive matching.
        s"""(?i)[(<\\[]?${Regex.quote(email)}[)>\\]]?""".r.replaceAllIn(name, "")
    ).map(name =>
        // filter out names with @ too close to the email address
        if (name.contains("@") && levenshteinSimilarity(name.toLowerCase(Locale.ROOT), email.toLowerCase(Locale.ROOT)) >= emailNameFilterThreshold) {
          ""
        } else {
          name
        }
    ).map(name => Normalization.normalizeWhitespace(name.trim()))
      .filter(_.nonEmpty)
      .map(valueFactory.createLiteral)
  }

  private def levenshteinSimilarity(s: String, t: String) = {
    val distance = StringUtils.getLevenshteinDistance(s, t)
    1.0f - (distance.toFloat / Math.max(s.length, t.length))
  }
}
