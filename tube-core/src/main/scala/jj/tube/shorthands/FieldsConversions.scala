package jj.tube.shorthands

import cascading.tuple.Fields

trait FieldsConversions {
  def f(name: String*): Fields = new Fields(name: _*)

  implicit def toField(fields: String): Fields = f(fields)
  implicit def toField(fields: List[String]): Fields = new Fields(fields: _*)
  implicit def toField(fields: Array[String]): Fields = new Fields(fields.asInstanceOf[Array[Comparable[Any]]]: _*)
  implicit def toField(product: Product): Fields = {
    val seq = product.productIterator.collect[String]({
      case f: String => f
    }).toList
    new Fields(seq: _*)
  }
}
