package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import cascading.flow.FlowDef

@RunWith(classOf[JUnitRunner])
class FilterTest extends FunSuite with BaseFlowTest with Matchers{
  test("should filter input longer then 3 signs"){
    val in = inTap("word", List("a","abc","abcd","ab"))
    val out = outTap

    val inputWords = Tube("words")
      .filter(){
        row => row("word").length() > 3
      }

    runFlow(FlowDef.flowDef
      .addSource(inputWords, in)
      .addTailSink(inputWords, out))

    out.content should contain only ("a", "abc", "ab")
  }
}
