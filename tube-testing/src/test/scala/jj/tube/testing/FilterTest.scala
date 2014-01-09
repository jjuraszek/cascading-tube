package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import cascading.flow.FlowDef
import jj.tube.testing.BaseFlowTest.Source

@RunWith(classOf[JUnitRunner])
class FilterTest extends FunSuite with BaseFlowTest with Matchers{
  test("should filter input longer then 3 signs"){
    //given
    val in = Source("word", List("a","abc","abcd","ab"))

    //when
    val inputWords = Tube("words")
      .filter() { row =>
        row("word").length() > 3
      }

    //then
    runFlow
      .withSource(inputWords, in)
      .withOutput(inputWords, {
        _ should contain only ("a", "abc", "ab")
      }).compute
  }
}
