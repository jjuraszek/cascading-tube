package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import jj.tube.testing.BaseFlowTest.Source

@RunWith(classOf[JUnitRunner])
class JoinTest extends FunSuite with BaseFlowTest with Matchers {
  test("join two individual based on id to get the age") {
    //given
    val srcNames = Source(("id1", "name"), List(
      ("2", "dijkstra"),
      ("1", "hawking")))
    val srcAge = Source(("id2", "age"), List(
      ("1", "16"),
      ("2", "17")))

    //when
    val inputAges = Tube("ages")
    val inputNames = Tube("names")
    val outputNamesWithAges = Tube("nameWithAges", inputNames)
      .join(inputAges).on("id1","id2")
      .retain("name", "age")

    //then
    val result = runFlow
      .withSource(inputNames, srcNames)
      .withSource(inputAges, srcAge)
      .withTailSink(outputNamesWithAges)
      .compute

    result(outputNamesWithAges).content should contain only("hawking,16", "dijkstra,17")
  }

  test("hash join two individual based on id to get the age") {
    //given
    val srcNames = Source(("id", "name"), List(
      ("2", "dijkstra"),
      ("1", "hawking")))
    val srcAge = Source(("id", "age"), List(
      ("1", "16"),
      ("2", "17")))

    //when
    val inputAges = Tube("ages")
    val inputNames = Tube("names")
    val outputNamesWithAges = Tube("nameWithAges", inputNames)
      .hashJoin(inputAges).on("id","id").withOutputScheme("id1","name","id2","age")
      .retain("name", "age")

    val result = runFlow
      .withSource(inputNames, srcNames)
      .withSource(inputAges, srcAge)
      .withTailSink(outputNamesWithAges)
      .compute

    //then
    result(outputNamesWithAges).content should contain only("hawking,16", "dijkstra,17")
  }
}
