package com.thymeflow.utilities.text

import java.text.Normalizer

import scala.annotation.tailrec

/**
  * @author David Montoya
  */
object Normalization {

  private val whitespaceRegex = """\s""".r

  def removeDiacriticalMarks(text: String) = {
    Normalizer.normalize(text, Normalizer.Form.NFD).replaceAll("[\\p{InCombiningDiacriticalMarks}]", "")
  }

  def normalizeWhitespace(text: String) = {
    whitespaceRegex.replaceAllIn(text, " ")
  }

  /***
    * Removes leading and trailing simple or double quotes from a string
    *
    * @param raw string to trim
    * @return trimmed string
    */

  def trimQuotes(raw: String): String = {
    @tailrec
    def rec(start: Int = 0, end: Int = raw.length): String = {
      if (end - start >= 2) {
        val startCharacter = raw.charAt(start)
        val endCharacter = raw.charAt(end - 1)
        if ((startCharacter == '\'' && endCharacter == '\'') || (startCharacter == '"' && endCharacter == '"')) {
          rec(start + 1, end - 1)
        } else {
          raw.substring(start, end)
        }
      } else {
        raw.substring(start, end)
      }
    }
    rec()
  }
}
