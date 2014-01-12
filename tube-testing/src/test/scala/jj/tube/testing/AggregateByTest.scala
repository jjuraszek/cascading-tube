package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import cascading.flow.FlowDef
import jj.tube.testing.BaseFlowTest.Source

@RunWith(classOf[JUnitRunner])
class AggregateByTest extends FunSuite with BaseFlowTest with Matchers{
  test("should support every of the casual operators: avg, sum, max, min, count"){
    //given
    val in = Source(("no","num"), List(
      ("1","3"),("1","1"),("1","2"),
      ("2","1")
    ))

    //when
    val inputNumbers = Tube("numbers")
      .aggregate("no")
        .avg("num","avg")
        .sum[Int]("num","sum")
        .max("num","max")
        .min("num","min")
        .count("count")
      .go

    //then
    runFlow
      .withSource(inputNumbers, in)
      .withOutput(inputNumbers, {
        _ should contain only ("1,2.0,6,3,1,3","2,1.0,1,1,1,1")
      }).compute
  }

  test("should take result type double if there is no type defined"){
    //given
    val in = Source("no", List("2"))

    //when
    val inputNumbers = Tube("numbers")
      .aggregate("no")
      .sum("no","sum")
      .go

    //then
    runFlow
      .withSource(inputNumbers, in)
      .withOutput(inputNumbers, {
      _ should contain only "2,2.0"
    }).compute
  }

  test("should take first element from stream according to desc order"){
    //given
    val in = Source(("no","weight","weightInInt"), List(("2","3","3"),("2","14","14"),("2","1","1")))

    //when
    val inputNumbers = Tube("numbers")
      .coerce[Int]("weightInInt")
      .aggregate("no")
      .first(DESC("weight"))
      .first(DESC("weightInInt"))
      .go

    //then
    runFlow
      .withSource(inputNumbers, in)
      .withOutput(inputNumbers, {
      _ should contain only "2,3,14"
    }).compute
  }
}
