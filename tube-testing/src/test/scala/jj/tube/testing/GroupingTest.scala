package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import jj.tube.testing.BaseFlowTest.Source

@RunWith(classOf[JUnitRunner])
class GroupingTest extends FunSuite with BaseFlowTest with Matchers {
  test("word count with alphabetic order of result"){
    //given
    val srcWords = Source("w", List("dog","cat","dog", "cat", "cat", "avocado"))

    //when
    val inputWords = Tube("words")
      .groupBy("w").map{ (group, row) =>
        val count = row.count(_ => true)
        List(Map("w" -> group.head._2, "c" -> count))
      }.declaring("w","c")
      .retain("w","c")

    //then
    runFlow
      .withSource(inputWords, srcWords)
      .withOutput(inputWords, {
        _ should contain only("dog,2", "cat,3", "avocado,1")
      }).compute

  }
}
