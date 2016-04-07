package thymeflow.text.alignment

import org.scalatest._

/**
  * @author David Montoya
  */
class AlignmentSpec extends FlatSpec with Matchers {
  // TODO: elaborate these test cases
  "Alignment" should "do handle multiple different alignments" in {
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

    val a1 = Alignment.alignment(Seq("JOHN", "SMITH"), "SMITH.JOHN", filter)
  }
}
