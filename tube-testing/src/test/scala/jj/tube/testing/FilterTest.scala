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
    val in = Source("word", List("a","abc","abcd","ab"))

    val inputWords = Tube("words")
      .filter() { row =>
        row("word").length() > 3
      }

    val result = runFlow
      .withSource(inputWords, in)
      .withTailSink(inputWords)
      .compute

    result(inputWords).content should contain only ("a", "abc", "ab")
  }
}
