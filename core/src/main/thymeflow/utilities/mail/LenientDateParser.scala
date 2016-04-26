package thymeflow.utilities.mail

import java.time._
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.Temporal
import java.time.zone.ZoneRulesException
import java.util.Locale

import com.typesafe.scalalogging.StrictLogging
import thymeflow.utilities.timezone.Abbreviation


/**
  * @author  David Montoya
  */

/**
  * Allows parsing of non-standard Internet Message RFC dates
  */
object LenientDateParser extends StrictLogging {

  type DateElementParser = (Seq[String], Option[Locale], PartialDate) => Option[(Seq[String], Option[Locale], PartialDate)]
  val yearMonthDayNumberPattern = """^[0-9]+$""".r
  val monthPattern = """^([^0-9]+)?$""".r
  val hourMinuteSecondPattern = """^([0-9]{1,2}):([0-9]{1,2})(?::([0-9]{1,2}))?$""".r
  val offsetPattern = """^([+-]?)([0-9]{1,})([0-9]{2})$""".r
  val timeZoneTrailing = """^"|"$""".r
  val rfc850DatePattern = """^([0-9]+)-(?:([0-9]+)|([a-z]+))-([0-9]+)$""".r
  val parenthesisPattern = """[()]|[^()]+""".r
  val elementParsers = Vector(
    (parts: Seq[String], localeOption: Option[Locale], partialDate: PartialDate) => {
      val head = parts.head
      val headCommaIndex = head.indexOf(",")
      if (headCommaIndex >= 0) {
        val (left, right) = head.splitAt(headCommaIndex)
        val tail = if (right.length == 1) parts.tail else right.tail +: parts.tail
        Some((tail, guessWeekDayLocale(left), partialDate))
      } else if (letters.findFirstIn(toLowerCase(head)).nonEmpty) {
        Some((parts.tail, guessWeekDayLocale(parts.head), partialDate))
      } else {
        // no day of week
        Some((parts, None, partialDate))
      }
    },
    (parts: Seq[String], localeOption: Option[Locale], partialDate: PartialDate) => {
      // RFC 850 date, deprecated
      rfc850DatePattern.findFirstMatchIn(toLowerCase(parts.head)) match {
        case Some(d) =>
          try {
            val first = Integer.parseInt(d.group(1))
            val third = Integer.parseInt(d.group(4))
            Option(d.group(2)) match {
              case Some(monthString) =>
                val month = Integer.parseInt(monthString)
                Some((parts.tail, localeOption, partialDate.copy(year = Some(first), month = Some(month), day = Some(third))))
              case None =>
                val month = monthsByLocale(Locale.US)(d.group(3))
                Some((parts.tail, Some(Locale.US), partialDate.copy(year = Some(third), month = Some(month), day = Some(first))))
            }
          } catch {
            case e: NumberFormatException =>
              Some((parts, localeOption, partialDate))
          }
        case None => Some((parts, localeOption, partialDate))
      }
    },
    (parts: Seq[String], localeOption: Option[Locale], partialDate: PartialDate) => {
      yearMonthDayNumberPattern.findFirstIn(parts.head).map {
        case dayString =>
          val day = Integer.parseInt(dayString)
          (parts.tail, localeOption, partialDate.copy(day = Some(day)))
      }
    },
    (parts: Seq[String], localeOption: Option[Locale], partialDate: PartialDate) => {
      monthPattern.findFirstIn(toLowerCase(parts.head)) match {
        case Some(monthStringLowerCase) =>
          val adjustedLocale = localeOption match {
            case Some(locale) => locale
            case None => guessMonthLocale(monthStringLowerCase).getOrElse(Locale.US)
          }
          val monthReplaced = localeReplacements(adjustedLocale, monthStringLowerCase)
          val monthOption =
            monthsByLocale(adjustedLocale).get(monthReplaced) match {
              case Some(month) => Some(month)
              case None => fullMonthsByLocale(adjustedLocale).get(monthReplaced)
            }
          monthOption match {
            case Some(month) => Some((parts.tail, Some(adjustedLocale), partialDate.copy(month = monthOption)))
            case None =>
              None
          }
        case None =>
          yearMonthDayNumberPattern.findFirstIn(parts.head) match {
            case Some(monthString) =>
              val month = Integer.parseInt(monthString)
              Some((parts.tail, localeOption, partialDate.copy(month = Some(month))))
            case None => None
          }
      }
    },
    (parts: Seq[String], localeOption: Option[Locale], partialDate: PartialDate) => {
      yearMonthDayNumberPattern.findFirstIn(parts.head) match {
        case Some(yearString) =>
          val year = Integer.parseInt(yearString)
          val adjustedYear = if (year < 100) {
            // the year is between 1969 and 1999 (inclusive)
            if (year > 68) {
              year + 1900
              // the year is between 2000 and 2068 (inclusive)
            } else {
              year + 2000
            }
          } else {
            year
          }
          if (adjustedYear > 1960) {
            Some((parts.tail, localeOption, partialDate.copy(year = Some(adjustedYear))))
          } else {
            None
          }
        case None => None
      }
    },
    (parts: Seq[String], localeOption: Option[Locale], partialDate: PartialDate) => {
      hourMinuteSecondPattern.findFirstMatchIn(parts.head).map {
        case hourMinuteSecond =>
          val hour = Integer.parseInt(hourMinuteSecond.group(1))
          val minute = Integer.parseInt(hourMinuteSecond.group(2))
          val second = Option(hourMinuteSecond.group(3)).map(Integer.parseInt).getOrElse(0)
          (parts.tail, localeOption, partialDate.copy(hour = Some(hour), minute = Some(minute), second = Some(second)))
      }
    },
    (parts: Seq[String], localeOption: Option[Locale], partialDate: PartialDate) => {
      val offsetCandidate = parts.head.replace("+-", "+")
      offsetPattern.findFirstMatchIn(offsetCandidate).map {
        case offset =>
          val signString = offset.group(1)
          val sign = if (signString == "-") -1 else 1
          val hour = Integer.parseInt(offset.group(2)) * sign
          val minute = Integer.parseInt(offset.group(3)) * sign
          (parts.tail, localeOption, partialDate.copy(offset = Some(ZoneOffset.ofHoursMinutes(hour, minute))))
      }
    },
    (parts: Seq[String], localeOption: Option[Locale], partialDate: PartialDate) => {
      def get(timeZoneCandidate: String) = {
        try {
          Some(ZoneId.of(timeZoneCandidate))
        } catch {
          case e: DateTimeException => None
          case e: ZoneRulesException => None
        }
      }
      val timezoneId = timeZoneTrailing.replaceAllIn(parts.head, "")

      def resolveAbbreviation() = {
        Abbreviation.groupedAbbrevations.get(timezoneId.toUpperCase(Locale.ROOT)).flatMap(_.headOption) match {
          case Some(abbreviation) =>
            try {
              // TODO: handle timezone better
              Some((parts.tail, localeOption, partialDate.copy(timezone = Some(ZoneId.of(abbreviation.offset)))))
            } catch {
              case e: DateTimeException =>
                None
              case e: ZoneRulesException =>
                None
            }
          case None => None
        }
      }
      def resolveSimple() = {
        val timeZoneCandidate = get(timezoneId)
        if (timeZoneCandidate.nonEmpty) {
          Some((parts.tail, localeOption, partialDate.copy(timezone = timeZoneCandidate)))
        } else {
          None
        }
      }
      def resolveTrailing() = {
        if (parts.length >= 2) {
          val timeZoneCandidate2 = get(timeZoneTrailing.replaceAllIn(parts.mkString(" "), ""))
          if (timeZoneCandidate2.nonEmpty) {
            Some(Vector(), localeOption, partialDate.copy(timezone = timeZoneCandidate2))
          } else {
            None
          }
        } else {
          None
        }
      }

      Vector(resolveAbbreviation _, resolveSimple _, resolveTrailing _).view.map(_ ()).collectFirst({
        case Some(result) => result
      })
    }

  )
  private val letters = """^[a-z]+\.?$""".r
  private val spacePattern = """[ ]+""".r
  private val weekDays = DayOfWeek.values().toVector
  private val months = Month.values().toVector
  private val dayOfWeekFormatterBuilder = new DateTimeFormatterBuilder().appendPattern("EEE")
  private val monthsFormatterBuilder = new DateTimeFormatterBuilder().appendPattern("MMM")
  private val fullMonthsFormatterBuilder = new DateTimeFormatterBuilder().appendPattern("MMMM")
  private val locales = Vector(Locale.US, Locale.FRANCE)
  private val dayOfWeekFormatters = locales.map(locale => dayOfWeekFormatterBuilder.toFormatter(locale))
  private val dayNamesByLocale = dayOfWeekFormatters.map(
    formatter =>
      formatter.getLocale -> weekDays.zipWithIndex.map {
        case (weekDay, index) => toLowerCase(formatter.format(weekDay)) -> index
      }.toMap
  ).toMap
  private val monthFormatters = locales.map(locale => monthsFormatterBuilder.toFormatter(locale))
  private val monthsByLocale = monthFormatters.map(
    formatter =>
      formatter.getLocale -> months.zipWithIndex.map {
        case (month, index) => toLowerCase(formatter.format(month)) -> (index + 1)
      }.toMap
  ).toMap
  private val fullMonthFormatters = locales.map(locale => fullMonthsFormatterBuilder.toFormatter(locale))
  private val fullMonthsByLocale = fullMonthFormatters.map(
    formatter =>
      formatter.getLocale -> months.zipWithIndex.map {
        case (month, index) => toLowerCase(formatter.format(month)) -> (index + 1)
      }.toMap
  ).toMap

  def localeReplacements(locale: Locale, monthString: String) = {
    if (locale == Locale.FRANCE) {
      monthString.replace("?", "\u00fb").replace("\u001a", "\u00e9").replace("ï¿½", "\u00fb")
    } else {
      monthString
    }
  }

  def parse(dateString: String): Option[Temporal] = {
    val tokens = for (m <- parenthesisPattern.findAllMatchIn(dateString)) yield {
      m.group(0) match {
        case "(" =>
          Open
        case ")" =>
          Close
        case s => Text(s)
      }
    }
    val (currentSegments, insideParenthesis, _) = tokens.foldLeft((Vector[String](), Vector[String](), 0))({
      case ((currentSegments, insideParenthesis, nestLevel), token) =>
        token match {
          case Open if nestLevel == 0 =>
            assert(insideParenthesis.isEmpty)
            (currentSegments, Vector(), 1)
          case Close if nestLevel == 1 =>
            (currentSegments :+ insideParenthesis.mkString(""), Vector(), 0)
          case Close if nestLevel > 1 =>
            (currentSegments, insideParenthesis :+ ")", nestLevel - 1)
          case Open =>
            // nestLevel >= 1
            assert(nestLevel >= 1)
            (currentSegments, insideParenthesis :+ "(", nestLevel + 1)
          case Close =>
            // nestLevel == 0
            assert(nestLevel == 0)
            assert(insideParenthesis.isEmpty)
            (currentSegments, insideParenthesis, nestLevel)
          case Text(content) if nestLevel >= 1 =>
            (currentSegments, insideParenthesis :+ content, nestLevel)
          case Text(content) =>
            // nestLevel == 0
            assert(nestLevel == 0)
            assert(insideParenthesis.isEmpty)
            (currentSegments ++ spacePattern.split(content).toVector, insideParenthesis, nestLevel)
        }
    })
    val segments = (currentSegments :+ insideParenthesis.mkString("")).filter(_.nonEmpty)
    // assert(segments.length > 0)
    val partialDate = parseDatePartial(segments, None, PartialDate())
    (partialDate.year, partialDate.month, partialDate.day, partialDate.hour, partialDate.minute, partialDate.second) match {
      case (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) =>
        val localDateTime = LocalDateTime.of(year, month, day, hour, minute, second)
        partialDate.offset match {
          case Some(offset) => Some(OffsetDateTime.of(localDateTime, offset))
          case None => partialDate.timezone match {
            case Some(timezone) =>
              Some(ZonedDateTime.of(localDateTime, timezone))
            case None =>
              Some(localDateTime)
          }
        }
      case _ =>
        None
    }
  }

  private def parseDatePartial(parts: Seq[String], localeOption: Option[Locale], partialDate: PartialDate): PartialDate = {
    val parsers = elementParsers.toBuffer
    var args = (parts, localeOption, partialDate)
    while (args._1.nonEmpty) {
      val result = parsers.view.zipWithIndex.map {
        case (parser, index) =>
          parser.tupled(args).map(x => (parser, index, x))
      }.collectFirst {
        case Some((parser, index, x)) =>
          parsers.remove(index)
          args = x
          ()
      }
      if (result.isEmpty) {
        args = args.copy(_1 = args._1.tail)
      }
    }
    args._3
  }

  private def guessWeekDayLocale(weekDayString: String): Option[Locale] = {
    val weekDayStringLowerCase = toLowerCase(weekDayString)
    for ((locale, dayNames) <- dayNamesByLocale) {
      if (dayNames.contains(weekDayStringLowerCase)) {
        return Some(locale)
      }
    }
    None
  }

  private def guessMonthLocale(monthString: String): Option[Locale] = {
    val monthStringLowerCase = toLowerCase(monthString)
    for ((locale, months) <- monthsByLocale) {
      if (months.contains(monthStringLowerCase)) {
        return Some(locale)
      }
    }
    None
  }

  private def toLowerCase(str: String) = str.toLowerCase(Locale.ROOT)

  sealed trait DateToken

  case class PartialDate(year: Option[Int] = None,
                         month: Option[Int] = None,
                         day: Option[Int] = None,
                         hour: Option[Int] = None,
                         minute: Option[Int] = None,
                         second: Option[Int] = None,
                         offset: Option[ZoneOffset] = None,
                         timezone: Option[ZoneId] = None)

  case class Text(content: String) extends DateToken

  case object Close extends DateToken

  case object Open extends DateToken

}
