package pkb.sync.converter.utils

import java.util.Locale

import org.apache.commons.lang3.StringUtils
import org.openrdf.model.{Literal, ValueFactory}
import pkb.utilities.text.Normalization

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
    Some(Normalization.trimQuotes(rawName)).map{
      case name =>
        // Filter common source of imprecision, email addresses contained within names,
        // between parenthesis, brackets or angular brackets. Case insensitive matching.
        s"""(?i)[(<\\[]?${Regex.quote(email)}[)>\\]]?""".r.replaceAllIn(name, "")
    }.map{
      case name =>
        // filter out names with @ too close to the email address
        if (name.contains("@") && levenshteinSimilarity(name.toLowerCase(Locale.ROOT), email.toLowerCase(Locale.ROOT)) >= emailNameFilterThreshold) {
          ""
        }else{
          name
        }
    }.map{
      // normalize whitespace and remove leading/trailing whitespace
      case name => Normalization.normalizeWhitespace(name.trim())
    }.filter{
      // remove empty names
      case name => name.nonEmpty
    }.map{
      case name =>
        valueFactory.createLiteral(name)
    }
  }

  private def levenshteinSimilarity(s: String, t: String) = {
    val distance = StringUtils.getLevenshteinDistance(s, t)
    1.0f - (distance.toFloat / Math.max(s.length, t.length))
  }
}
