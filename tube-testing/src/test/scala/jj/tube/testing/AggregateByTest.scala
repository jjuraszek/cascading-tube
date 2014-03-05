package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers
import jj.tube._
import jj.tube.testing.BaseFlowTest.Source

@RunWith(classOf[JUnitRunner])
class AggregateByTest extends FunSuite with BaseFlowTest with Matchers{
  test("should count only not null"){
    //given
    val in = Source(("no","letter"), List(
      ("1","a"),("1","b"),("1",null),
      ("2","c"),("2",null)
    ))

    //when
    val inputNumbers = Tube("numbers")
      .aggregateBy("no").countIgnoringNull("letter", "out").go

    //then
    runFlow
      .withSource(inputNumbers, in)
      .withOutput(inputNumbers, {
      _ should contain only ("1,2","2,1")
    }).compute
  }

  test("should support every of the casual operators: avg, sum, max, min, count"){
    //given
    val in = Source(("no","num"), List(
      ("1","3"),("1","1"),("1","2"),
      ("2","1")
    ))

    //when
    val inputNumbers = Tube("numbers")
      .aggregateBy("no").withThreshold(10)
        .avg("num","avg")
        .sum[Int]("num","sum")
        .max("num","max")
        .min("num","min")
        .count("count").go

    //then
    runFlow
      .withSource(inputNumbers, in)
      .withOutput(inputNumbers, {
        _ should contain only ("1,2.0,6,3,1,3","2,1.0,1,1,1,1")
      }).compute
  }

  test("should support every of the casual operators: avg, sum, max, min with default output set to input"){
    //given
    val in = Source(("no","n1","n2","n3","n4"), List(("1","1","2","3","4")))

    //when
    val inputNumbers = Tube("numbers")
      .aggregateBy("no")
        .avg("n1")
        .sum[Int]("n2")
        .max("n3")
        .min("n4")
      .go

    //then
    runFlow
      .withSource(inputNumbers, in)
      .withOutput(inputNumbers, {
        _ should contain only "1,1.0,2,3,4"
      }).compute
  }

  test("should take result type double if there is no type defined"){
    //given
    val in = Source("no", List("2"))

    //when
    val inputNumbers = Tube("numbers")
      .aggregateBy("no")
      .sum("no","sum")


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
      .aggregateBy("no")
      .first(DESC("weight"))
      .first(DESC("weightInInt"))


    //then
    runFlow
      .withSource(inputNumbers, in)
      .withOutput(inputNumbers, {
      _ should contain only "2,3,14"
    }).compute
  }
}
