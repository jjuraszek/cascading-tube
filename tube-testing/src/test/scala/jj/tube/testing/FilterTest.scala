package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import jj.tube.testing.BaseFlowTest.Source

@RunWith(classOf[JUnitRunner])
class FilterTest extends FunSuite with BaseFlowTest with Matchers{
  test("should filter input longer then 3 signs"){
    //given
    val in = Source("word", List("a","abc","abcd","ab"))

    //when
    val inputWords = Tube("words")
      .filter{ row =>
        val word = row("word")
        word.length() < 3
      }.go

    //then
    runFlow
      .withSource(inputWords, in)
      .withOutput(inputWords, {
        _ should contain only ("a", "abc", "ab")
      }).compute
  }

  test("should NOT filter input longer then 3 signs"){
    //given
    val in = Source(("idx","word"), List(("1","a"),("1","abc"),("1","abcd"),("1","ab")))

    //when
    val inputWords = Tube("words")
      .filterNot{ row =>
      val word = row("word")
      !(word.length() > 3 && row.size() == 1)
    }.withInput("word")
    .retain("word")

    //then
    runFlow
      .withSource(inputWords, in)
      .withOutput(inputWords, {
      _ should contain only "abcd"
    }).compute
  }
}
