package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, FunSuite}
import jj.tube._
import cascading.tuple.TupleEntry
import scala.collection.convert.WrapAsScala.asScalaIterator

@RunWith(classOf[JUnitRunner])
class RichTupleEntryTest extends FunSuite with Matchers {
  test("should extend tuple entry with new field"){
    //given
    val tupleEntry:TupleEntry = Map("a" -> "b")
    //when
    val result = tupleEntry.add("c"->"d")
    //then
    List("a","c") should be (result.getFields.iterator().toList.map(_.toString))
    List("b","d") should be (result.getTuple.iterator().toList.map(_.toString))
  }

  test("should update tuple entry"){
    //given
    val tupleEntry:TupleEntry = Map("a" -> "b")
    //when
    val result = tupleEntry.add("a"->"c")
    //then
    List("c") should be (result.getTuple.iterator().toList.map(_.toString))
  }

  test("should add all map entries"){
    //given
    val tupleEntry:TupleEntry = Map("0" -> "1","a" -> "b")
    //when
    val result = tupleEntry.addAll(Map("a"->"c", "b" -> "d"))
    //then
    List("0","a","b") should be (result.getFields.iterator().toList.map(_.toString))
    List("1","c","d") should be (result.getTuple.iterator().toList.map(_.toString))
  }
}