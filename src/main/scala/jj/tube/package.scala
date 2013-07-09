package jj

import cascading.tuple.Fields

package object tube {
  //fields conversions
  implicit def aggregateFields(fields: Seq[Fields]):Fields = fields.reduceLeft[Fields]( (f1,f2) => f1.append(f2))
  implicit def toField(fields: Seq[String]):Fields = new Fields(fields:_*)
  implicit  def tooField(product:Product):Fields = {
    val seq=product.productIterator.collect[String]({case f:String => f}).toList
    new Fields(seq: _*)
  }
}