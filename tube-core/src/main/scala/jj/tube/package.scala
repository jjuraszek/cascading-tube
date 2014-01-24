package jj

import cascading.tuple.{TupleEntry, Fields, Tuple}
import cascading.pipe.Pipe
import jj.tube.builders.OperationBuilder
import java.util.Comparator

/**
 * Object containing helper method for operating on input and output of the flow. Incorporating standard conversions between scala structures and cascading.
 */
package object tube extends OperationShortcuts with SortShortcut{
  implicit def toPipe(tube: Tube) = tube.pipe
  implicit def toTube(pipe: Pipe) = new Tube(pipe)

  type FUNCTION = TupleEntry => List[TupleEntry]
  type BUFFER = (TupleEntry, Iterator[TupleEntry]) => List[TupleEntry]
  type FILTER = TupleEntry => Boolean

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

    def toMap = (0 until tupleEntry.getFields.size).map{ i =>
      tupleEntry.getFields.get(i).toString -> Option(tupleEntry.getObject(i)).getOrElse("").toString
    }.toMap
  }
}

trait OperationShortcuts extends FieldsConversions{
  /** finalize builder by applying it to Tube */
  implicit def backToTube(builder: OperationBuilder) = builder.go

  implicit def toTupleEntryList(schemeWithValues: List[Map[String, String]]) =
    schemeWithValues.map(toTupleEntry)

  implicit def toTupleEntryList(schemeWithValues: Map[String, Any]) =
    List(toTupleEntry(schemeWithValues))

  implicit def toTupleEntry(schemeWithValues: Map[String, Any]):TupleEntry =
    schemeWithValues.foldLeft(new TupleEntry(schemeWithValues.keys.toList, Tuple.size(schemeWithValues.size))) {
      (te, entry) =>
        entry._2 match {
          case x: Boolean => te.setBoolean(entry._1, x)
          case x: Int => te.setInteger(entry._1, x)
          case x: Double => te.setDouble(entry._1, x)
          case x => te.setString(entry._1, if (x != null) x.toString else "")
        }
        te
    }
}

trait SortShortcut{
  /**
   * Define the sort order for declared fields and apply correct comparator for them
   * @param reverse
   */
  //TODO allow custom comparators by order builder
  sealed case class SortOrder(sortedFields: Fields, reverse: Boolean = false) {
    (0 until sortedFields.size).foreach {
      sortedFields.setComparator(_, new Comparator[Comparable[Any]] with Serializable {
        def compare(left: Comparable[Any], right: Comparable[Any]): Int = {
          if (reverse) right compareTo left
          else left compareTo right
        }
      })
    }

    val isAscending = !reverse
  }

  /** create desc order for fields*/
  def DESC(sortedFields: Fields) = SortOrder(sortedFields, reverse = true)
  /** create asc order for fields*/
  def ASC(sortedFields: Fields) = SortOrder(sortedFields, reverse = false)
}

@deprecated("to be remove in ver.4", "3.0.0")
trait TupleConversions extends FieldsConversions {
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

  @deprecated("to be remove in ver.4", "3.0.0")
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