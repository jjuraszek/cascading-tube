package jj

import cascading.tuple.Fields

package object tube {
  //fields conversions
  implicit def aggregateFields(fields: Seq[Fields]):Fields = fields.reduceLeft[Fields]( (f1,f2) => f1.append(f2))
  implicit def stringToField(fields: Seq[String]):Fields = new Fields(fields:_*)
}