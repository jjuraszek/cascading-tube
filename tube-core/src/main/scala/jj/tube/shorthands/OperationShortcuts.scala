package jj.tube.shorthands

import jj.tube.builders.OperationBuilder
import cascading.tuple.{Fields, Tuple, TupleEntry}

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

  implicit def toSimpleTupleEntry(entry: Seq[Any]) = List(tuple(entry))

  implicit def toSimpleTupleEntryList(entries: Iterator[Seq[Any]]) =
    entries.map(tuple).toList

  def tuple(entry: Seq[Any]):TupleEntry = new TupleEntry(Fields.UNKNOWN, entry.foldLeft(new Tuple){
    (tup, nextVal) => {
      tup.add(nextVal)
      tup
    }
  })
}
