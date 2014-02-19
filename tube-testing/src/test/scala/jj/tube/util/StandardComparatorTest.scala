package jj.tube.util

import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner
import cascading.tuple.TupleEntry

@RunWith(classOf[JUnitRunner])
class StandardComparatorTest extends FunSuite with Matchers{
  test("should compare strings"){
    //given
    val stdComparator = new StandardComparator(false)
    //when
    val res = stdComparator.compare(
      "a".asInstanceOf[Comparable[Any]],
      "b".asInstanceOf[Comparable[Any]])
    //then
    res should be < 0
  }

  test("should compare null with string"){
    //given
    val stdComparator = new StandardComparator(false)
    //when
    val res = stdComparator.compare(
      null,
      "b".asInstanceOf[Comparable[Any]])
    //then
    res should be < 0
  }

  test("should compare string with null"){
    //given
    val stdComparator = new StandardComparator(false)
    //when
    val res = stdComparator.compare(
      "a".asInstanceOf[Comparable[Any]],
      null)
    //then
    res should be > 0
  }

  test("should reverse order"){
    //given
    val stdComparator = new StandardComparator(true)
    //when
    val res = stdComparator.compare(
      "a".asInstanceOf[Comparable[Any]],
      "b".asInstanceOf[Comparable[Any]])
    //then
    res should be > 0
  }

  test("should equal for both nulls"){
    //given
    val stdComparator = new StandardComparator(true)
    //when
    val res = stdComparator.compare(
      null,
      null)
    //then
    res should equal (0)
  }

  test("should equal for both same vals"){
    //given
    val stdComparator = new StandardComparator(true)
    //when
    val res = stdComparator.compare(
      "a".asInstanceOf[Comparable[Any]],
      "a".asInstanceOf[Comparable[Any]])
    //then
    res should equal (0)
  }
}
