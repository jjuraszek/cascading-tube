package jj

import cascading.tuple.TupleEntry
import cascading.pipe.Pipe
import org.json4s._
import org.json4s.native.JsonMethods._
import jj.tube.shorthands.{FieldsConversions, OperationShortcuts, SortShortcut}
import scala.collection.convert.WrapAsScala.asScalaIterator
import scala.collection.immutable.TreeMap
import scala.util.Try

/**
 * Object containing helper method for operating on input and output of the flow. Incorporating standard conversions between scala structures and cascading.
 */
package object tube extends FieldsConversions with OperationShortcuts with SortShortcut{
  implicit val jsonStandardFormats = DefaultFormats

  implicit def toPipe(tube: Tube) = tube.pipe
  implicit def toTube(pipe: Pipe) = new Tube(pipe)

  type FUNCTION = TupleEntry => List[TupleEntry]
  type BUFFER = (TupleEntry, Iterator[TupleEntry]) => TraversableOnce[TupleEntry]
  type FILTER = TupleEntry => Boolean
  type JOIN = (Iterator[TupleEntry], Iterator[TupleEntry]) => TraversableOnce[TupleEntry]

  /**allow easy operations on TupleEntry without allocation **/
  implicit class RichTupleEntry(val tupleEntry: TupleEntry) extends AnyVal {
    def safe[T](alias:String):Option[T] = Try(tupleEntry.getObject(alias).asInstanceOf[T]).toOption
    def safe[T](position:Int):Option[T] = Try(tupleEntry.getObject(position).asInstanceOf[T]).toOption

    def apply(alias:String) = safe[String](alias).get
    def apply(position:Int) = safe[String](position).get

    def int(alias:String) = safe[Int](alias).get
    def int(position:Int) = safe[Int](position).get

    def double(alias:String) = safe[Double](alias).get
    def double(position:Int) = safe[Double](position).get

    def json(alias:String) = parse(apply(alias))
    def json(position:Int) = parse(apply(position))

    def add(value: (String,Any)) = { tupleEntry.setObject(value._1, value._2); this}
    def addAll(extraFields: Map[String,Any]) = { extraFields.foreach(kv => tupleEntry.setObject(kv._1, kv._2)); this}

    def copy = new TupleEntry(tupleEntry)

    def toSortedMap = TreeMap(toMap.toArray:_*)

    def toMap = tupleEntry.getFields.iterator.map{ i =>
      i.toString -> Option(tupleEntry.getString(i.asInstanceOf[Comparable[_]])).getOrElse("")
    }.toMap
  }
}





