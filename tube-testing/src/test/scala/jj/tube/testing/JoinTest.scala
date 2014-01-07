package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import cascading.flow.FlowDef

@RunWith(classOf[JUnitRunner])
class JoinTest extends FunSuite with BaseFlowTest with Matchers {
  test("join two individual based on id to get the age") {
    //given
    val tapNames = inTap(("id1", "name"), List(
      ("2", "dijkstra"),
      ("1", "hawking")))
    val tapAge = inTap(("id2", "age"), List(
      ("1", "16"),
      ("2", "17")))
    val out = outTap

    //when
    val inputAges = Tube("ages")
    val inputNames = Tube("names")
    val outputNamesWithAges = Tube("nameWithAges", inputNames)
      .join(inputAges).on("id1","id2").getting()
      .retain("name", "age")

    runFlow(FlowDef.flowDef
      .addSource(inputNames, tapNames)
      .addSource(inputAges, tapAge)
      .addTailSink(outputNamesWithAges, out))

    //then
    out.content should contain only("hawking,16", "dijkstra,17")
  }

  test("hash join two individual based on id to get the age") {
    //given
    val tapNames = inTap(("id1", "name"), List(
      ("2", "dijkstra"),
      ("1", "hawking")))
    val tapAge = inTap(("id2", "age"), List(
      ("1", "16"),
      ("2", "17")))
    val out = outTap

    //when
    val inputAges = Tube("ages")
    val inputNames = Tube("names")
    val outputNamesWithAges = Tube("nameWithAges", inputNames)
      .hashJoin(inputAges).on("id1","id2").getting()
      .retain("name", "age")

    runFlow(FlowDef.flowDef
      .addSource(inputNames, tapNames)
      .addSource(inputAges, tapAge)
      .addTailSink(outputNamesWithAges, out))

    //then
    out.content should contain only("hawking,16", "dijkstra,17")
  }
}
