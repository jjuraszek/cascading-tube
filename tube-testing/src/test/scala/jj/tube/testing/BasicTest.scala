package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import jj.tube.testing.BaseFlowTest.Source

@RunWith(classOf[JUnitRunner])
class BasicTest extends FunSuite with BaseFlowTest with Matchers {
  test("replace char with double char and leave other fields without alteration"){
    //given
    val srcCharNums = Source(("num","c"), List(("2","a"),("3","b")))

    //when
    val inputCharNums = Tube("charsWithNums")
      .replace("c") { row =>
        Map("c" -> (row("c").toString + row("c").toString))
      }.retain("c","num")

    //then
    runFlow
      .withSource(inputCharNums, srcCharNums)
      .withOutput(inputCharNums, {
        _ should contain only("aa,2","bb,3")
      }).compute
  }
}
