package jj

import cascading.tuple.TupleEntry
import cascading.pipe.Pipe
import org.json4s._
import org.json4s.native.JsonMethods._
import jj.tube.shorthands.{FieldsConversions, OperationShortcuts, SortShortcut}

/**
 * Object containing helper method for operating on input and output of the flow. Incorporating standard conversions between scala structures and cascading.
 */
package object tube extends FieldsConversions with OperationShortcuts with SortShortcut{
  implicit val jsonStandardFormats = DefaultFormats

  implicit def toPipe(tube: Tube) = tube.pipe
  implicit def toTube(pipe: Pipe) = new Tube(pipe)

  type FUNCTION = TupleEntry => List[TupleEntry]
  type BUFFER = (TupleEntry, Iterator[TupleEntry]) => List[TupleEntry]
  type FILTER = TupleEntry => Boolean
  type JOIN = (Iterator[TupleEntry], Iterator[TupleEntry]) => List[TupleEntry]

  /**allow easy operations on TupleEntry without allocation **/
  implicit class RichTupleEntry(val tupleEntry: TupleEntry) extends AnyVal {
    def get[T](alias:String):T = tupleEntry.getObject(alias).asInstanceOf[T]
    def get[T](position:Int):T = tupleEntry.getObject(position).asInstanceOf[T]

    def apply(alias:String) = get[String](alias)
    def apply(position:Int) = get[String](position)

    def int(alias:String) = get[Int](alias)
    def int(position:Int) = get[Int](position)

    def double(alias:String) = get[Double](alias)
    def double(position:Int) = get[Double](position)

    def json(alias:String) = parse(get[String](alias))
    def json(position:Int) = parse(get[String](position))

    def +(value: (String,AnyRef)) = { tupleEntry.setObject(value._1, value._2)}

    def toMap = (0 until tupleEntry.getFields.size).map{ i =>
      tupleEntry.getFields.get(i).toString -> Option(tupleEntry.getObject(i)).getOrElse("").toString
    }.toMap
  }
}





