package thymeflow.text.alignment

import org.scalatest._

/**
  * @author David Montoya
  */
class AlignmentSpec extends FlatSpec with Matchers {
  // TODO: elaborate these test cases
  "Alignment" should "globally align two texts without crashing" in {
    def filter(score: Double, matches: Int, mismatches: Int) = {
      matches.toDouble / (matches + mismatches).toDouble >= 0.7
    }

    val a0 = Alignment.align(" ALICE SMITH", " JOHN DOE", filter)

    val p0 = Alignment.align(" SMI SMXTH", " SMI SMITH", filter)
    val p1 = Alignment.align(" SXXMITH", " SMITH", filter)
    val p2 = Alignment.align(" SMITHABCRT", " ABCRT SMITHXYZRT", filter)

    val s0 = Alignment.align(" J DOE", " J DOE", filter)
    val s1 = Alignment.align(" JOHN DOE", " J DOE", filter)
    val s2 = Alignment.align(" DOEDOESOME", " DOE DOESOME", filter)
    val s3 = Alignment.align(" JOHN DOE", " DOE JOHN", filter)
    val s4 = Alignment.align(" ALICE SMITD", " ALICE SMITH", filter)
    val s5 = Alignment.align(" ALICE SMIT", " ALICE SMITH", filter)
    val s6 = Alignment.align(" ALICA SMITH", " A SMITH", filter)

    val f1 = Alignment.find("JOHN", "JOHN DOE", filter)
    val f2 = Alignment.find("JOHN", "DOE JOHN", filter)
    val f3 = Alignment.find("JOHN", "XOHN", filter)
    val f4 = Alignment.find("JOHN", "JDOE", filter)
  }

  "Alignment" should "globally align some text with a sequence of terms" in {
    def filter(score: Double, matches: Int, mismatches: Int) = {
      matches.toDouble / (matches + mismatches).toDouble >= 0.7
    }
    val (_, a1) = Alignment.alignment(Seq("JOHN", "SMITH"), "SMITH.JOHN", filter)
    a1 should be(Vector(("JOHN", Vector(("JOHN", 6, 9))), ("SMITH", Vector(("SMITH", 0, 4)))))
    val (_, a2) = Alignment.alignment(Seq("gbbbg", "gaaaaaa"), "gbbbg.gaaaaaa", filter)
    a2 should be(Vector(("gbbbg", Vector(("gbbbg", 0, 4))), ("gaaaaaa", Vector(("gaaaaaa", 6, 12)))))
    // the following test case is not yet well specified
    val (_, a3) = Alignment.alignment(Seq("shdrahi", "brishrastomo"), "xxbrishra", filter)
    a3 should be(Vector(("shdrahi", Vector(("shra", 5, 8))), ("brishrastomo", Vector(("bri", 2, 4)))))
    val (_, a4) = Alignment.alignment(Seq("aaaaaa", "bbbbbbbbbbb", "aaaaaa"), "aaaaaa.bbbbbbbbbbb", filter)
    a4 should be(Vector(("aaaaaa", Vector(("aaaaaa", 0, 5))), ("bbbbbbbbbbb", Vector(("bbbbbbbbbbb", 7, 17))), ("aaaaaa", Vector())))
    val (_, a6) = Alignment.alignment(Seq("abbbbb", "aaaaaa"), "abbb", filter)
    a6 should be(Vector(("abbbbb", Vector(("abbb", 0, 3))), ("aaaaaa", Vector())))
    val (_, a7) = Alignment.alignment(Seq("corporate", "2", "nus", "relations"), "corpcommunications", filter)
  }
}
