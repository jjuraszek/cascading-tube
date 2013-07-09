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
}