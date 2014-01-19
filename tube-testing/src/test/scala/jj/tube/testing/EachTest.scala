package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import jj.tube.testing.BaseFlowTest.Source
import cascading.tuple.Fields

@RunWith(classOf[JUnitRunner])
class EachTest extends FunSuite with BaseFlowTest with Matchers {
  test("prefix range from 1  to num added to chars"){
    //given
    val srcCharNums = Source(("num","c"), List(("2","a"),("3","b")))

    //when
    val inputCharNums = Tube("charsWithNums")
      .coerce[Int]("num")
      .flatMap(Fields.ALL){ row =>
        val count = row[Int]("num")
        val c = row[String]("c")
        (1 to count).map { idx =>Map("w"->s"$c$idx")}.toList
      }.declaring("w")
      .withResult(Fields.RESULTS)
      .go

    //then
    runFlow
      .withSource(inputCharNums, srcCharNums)
      .withOutput(inputCharNums, {
        _ should contain only("a1","a2","b1","b2","b3")
      }).compute
  }
}
