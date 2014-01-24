package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import jj.tube.testing.BaseFlowTest.Source
import cascading.tuple.TupleEntry

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
    runFlow
      .withSource(inputNames, srcNames)
      .withSource(inputAges, srcAge)
      .withOutput(outputNamesWithAges, {
        _ should contain only("hawking,16", "dijkstra,17")
      }).compute

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
      .hashJoin(inputAges).on("id").declaring("id1","name","id2","age")
      .retain("name", "age")

    //then
    runFlow
      .withSource(inputNames, srcNames)
      .withSource(inputAges, srcAge)
      .withOutput(outputNamesWithAges, {
        _ should contain only("hawking,16", "dijkstra,17")
      }).compute
  }

  test("should implement strategy to join parent with child under age 18 or parent with info that he has no children"){
    //given
    val srcParent = Source(("name","id"), List(("joe","1"),("carol","2"),("sue","3")))
    val srcChildren = Source(("id","age"), List(("1","17"),("2","35"),("2","16")))

    //when
    val inputParents = Tube("parents")
    val inputChildren = Tube("children")
      .coerce[Int]("age")
    val ageOfOldestChildPerParent = Tube("parentWithChildAge",inputParents)
      .customJoin(inputChildren).on("id") { (parents, children) =>
        val parent = parents.next()
        val underAgeChildren = children.filter(_.int("age")<18)
        if(underAgeChildren.isEmpty)List(Map("name" -> parent("name"), "age" -> "NO_CHILD"))
        else underAgeChildren.map { row:TupleEntry =>
          toTupleEntry(Map("name" -> parent("name"), "age" -> row.int("age")))
        }.toList
      }.declaring("name","age")

    //then
    runFlow
      .withSource(inputParents, srcParent)
      .withSource(inputChildren, srcChildren)
      .withOutput(ageOfOldestChildPerParent, {
      _ should contain only("joe,17", "carol,16","sue,NO_CHILD")
    }).compute
  }
}
