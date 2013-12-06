package jj.base

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import cascading.flow.FlowDef

@RunWith(classOf[JUnitRunner])
class SampleTest extends FunSuite with CascadingFlowTest with Matchers{
  test("should filter input longer then 3 signs"){
    val inTap = in(Array("word"), Set(Array("a"),Array("abc"),Array("abcd"),Array("ab")))
    val outTap = out

    val inputWords = Tube("words")
      .filter(){
        row => row("word").length() > 3
      }

    runFlow(FlowDef.flowDef
      .addSource(inputWords, inTap)
      .addTailSink(inputWords, outTap))

    outTap.result should contain only ("a", "abc", "ab")
  }
}
