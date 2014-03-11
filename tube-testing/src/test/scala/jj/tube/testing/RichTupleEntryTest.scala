package jj.tube.testing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, FunSuite}
import jj.tube._
import cascading.tuple.TupleEntry
import scala.collection.convert.WrapAsScala.asScalaIterator
import org.json4s.JsonAST.{JNothing, JObject}
import scala.util.{Failure, Try}

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

  test("should convert int to double on the fly while getting value"){
    //given
    val tupleEntry:TupleEntry = Map("a" -> 1)
    //when
    val result = tupleEntry.double("a")
    //then
    "1.0" should be (result.toString)
  }

  test("should parse json correctly"){
    //given
    val tupleEntry:TupleEntry = Map("a" -> """{"a":"b"}""")
    //when
    val result = tupleEntry.json("a")
    //then
    assert(!JNothing.getClass.equals(result.getClass))
  }

  test("should throw exception for unparsable json"){
    //given
    val tupleEntry:TupleEntry = Map("a" -> """"{"c""")
    //when
    Try(tupleEntry.json("a")) shouldBe a [Failure[org.json4s.ParserUtil.ParseException]]
  }

  test("should return None option for null value of field"){
    //given
    val tupleEntry = new TupleEntry("a",cascading.tuple.Tuple.size(1))
    //when
    val result = tupleEntry.safeGet[String]("a")
    //then
    assert(result == None)
  }

  test("should return empty string for no value"){
    //given
    val tupleEntry = new TupleEntry("a",cascading.tuple.Tuple.size(1))
    //when
    val result = tupleEntry("a")
    //then
    assert(result == "")
  }
}
