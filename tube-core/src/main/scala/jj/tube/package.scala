package jj

import cascading.tuple.{TupleEntry, Fields, Tuple}
import cascading.tap.{SinkMode, Tap}
import cascading.tap.hadoop.Hfs
import cascading.scheme.hadoop.TextDelimited
import cascading.pipe.Pipe
import jj.tube.builders.OperationBuilder

/**
 * Object containing helper method for operating on input and output of the flow. Incorporating standard conversions between scala structures and cascading.
 */
package object tube extends TupleConversions{
  implicit def toPipe(tube: Tube) = tube.pipe

  implicit def toTube(pipe: Pipe) = new Tube(pipe)

  implicit def backToTube(builder: OperationBuilder) = builder.go

  sealed case class Order(dir:Boolean)
  val DESC = Order(true)
  val ASC = Order(false)
}

trait TupleConversions extends FieldsConversions {
  def toTupleEntry(schemeWithValues: Map[String, Any]) =
    schemeWithValues.foldLeft(new TupleEntry(schemeWithValues.keys.toList, Tuple.size(schemeWithValues.size))) {
      (te, entry) =>
        entry._2 match {
          case x: Boolean => te.setBoolean(entry._1, x)
          case x: Int => te.setInteger(entry._1, x)
          case x: Double => te.setDouble(entry._1, x)
          case x => te.setString(entry._1, if (x != null) x.toString() else "")
        }
        te
    }

  def toMap(tupleEntry: TupleEntry) = {
    val fieldWithVal = for {
      i <- 0 until tupleEntry.getFields.size
    } yield {
      tupleEntry.getFields.get(i).toString -> Option(tupleEntry.getObject(i)).getOrElse("").toString
    }
    fieldWithVal.toMap
  }

  def toTuple(row: List[Any]) = new cascading.tuple.Tuple(row.map(_.asInstanceOf[Object]): _*)

  def toList(tuple: Tuple) = (for (i <- 0 to tuple.size) yield tuple.getObject(i).toString).toList
}

trait FieldsConversions {
  def f(name: String*): Fields = new Fields(name: _*)

  implicit def aggregateFields(fields: Seq[Fields]): Fields = fields.reduceLeft[Fields]((f1, f2) => f1.append(f2))

  implicit def toField(fields: String): Fields = f(fields)

  implicit def toField(fields: List[String]): Fields = new Fields(fields: _*)

  implicit def toField(product: Product): Fields = {
    val seq = product.productIterator.collect[String]({
      case f: String => f
    }).toList
    new Fields(seq: _*)
  }
}