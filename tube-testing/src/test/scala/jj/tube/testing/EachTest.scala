package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import jj.tube.testing.BaseFlowTest.Source
import cascading.tuple.Fields
import org.json4s._
import org.json4s.native.JsonMethods._

@RunWith(classOf[JUnitRunner])
class EachTest extends FunSuite with BaseFlowTest with Matchers {
  test("prefix range from 1  to num added to chars"){
    //given
    val srcCharNums = Source(("num","c"), List(("2","a"),("3","b")))

    //when
    val inputCharNums = Tube("charsWithNums")
      .coerce[Int]("num")
      .flatMap(Fields.ALL){ row =>
        val count = row.int("num")
        val c = row("c")
        (1 to count).map {
          idx => Map("w"->s"$c$idx")
        }
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

  test("should extract json inner values"){
    //given
    val srcPerson = Source("j", List("""{
          "name": "joe",
          "address":{
            "name": "white tower",
            "street":"backer",
            "number": "12"
          }
        }"""))
    //when
    val inputPerson = Tube("person")
      .map(Fields.ALL){ row =>
          val j = row.json("j")
          tuple((j \ "name").extract[String],
                (j \\ "street")(0).extract[String])
        }.declaring("name", "street")
        .withResult(Fields.RESULTS)
      .go

    //then
    runFlow
      .withSource(inputPerson, srcPerson)
      .withOutput(inputPerson, {
      _ should contain only "joe,backer"
    }).compute
  }
}
