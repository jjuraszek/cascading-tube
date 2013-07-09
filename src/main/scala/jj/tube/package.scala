package jj

import cascading.tuple.{TupleEntry, Fields, Tuple}
import scala.collection.JavaConversions.seqAsJavaList

package object tube {
  //fields
  def f(name: String*): Fields = new Fields(name: _*)

  implicit def aggregateFields(fields: Seq[Fields]): Fields = fields.reduceLeft[Fields]((f1, f2) => f1.append(f2))

  implicit def toField(fields: String): Fields = f(fields)

  implicit def toField(fields: Seq[String]): Fields = new Fields(fields: _*)

  implicit def toField(product: Product): Fields = {
    val seq = product.productIterator.collect[String]({
      case f: String => f
    }).toList
    new Fields(seq: _*)
  }

  //tuple entry
  def t(values: Object*) = {
    new Tuple(values: _*)
  }

  implicit def toTuple(product: Product): Tuple = {
    new Tuple(seqAsJavaList(product.productIterator.toList.toSeq))
  }

  def tupleEntry(schemeWithValues: Map[String, String]) = {
    val te = new TupleEntry(toField(schemeWithValues.keys.toSeq), Tuple.size(schemeWithValues.size))
    schemeWithValues.foreach {
      entry =>
        te.setString(entry._1, entry._2)
    }
    te
  }

  def tupleEntry(fields: Fields) = {
    new TupleEntry(fields, Tuple.size(fields.size))
  }
}