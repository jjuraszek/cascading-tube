package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import jj.tube.testing.BaseFlowTest.Source
import jj.tube.builders.CoGroupingBuilder

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

  test("list number of childs for each parent with one coGroup operation"){
    //given
    val srcParent = Source(("name","id_parent"), List(("joe","1"),("carol","2")))
    val srcChildren = Source("parent", List("1","1","1","2","2"))

    //when
    val inputParents = Tube("parents")
    val inputChildren = Tube("children")
    val ageOfOldestChildPerParent = Tube("ageOfOldestChildPerParent",inputChildren)
      .coGroup(inputParents).on("parent","id_parent").map { (group, row) =>
        val firstChild = row.next()
        List(Map("name" -> firstChild("name"),"no"->(1 +row.count( _ => true))))
      }.declaring("name","no")
      .go

    //then
    runFlow
      .withSource(inputParents, srcParent)
      .withSource(inputChildren, srcChildren)
      .withOutput(ageOfOldestChildPerParent, {
        _ should contain only("joe,3", "carol,2")
      }).compute
  }
}
