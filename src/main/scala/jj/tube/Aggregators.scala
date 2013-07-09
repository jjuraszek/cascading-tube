package jj.tube

import cascading.pipe.assembly.{AverageBy, FirstBy, CountBy, SumBy}

object Aggregators {
  def sum(field: String, resultType: Class[_]): SumBy = new SumBy(field, field, resultType)

  def sum(field: String): SumBy = sum(field, classOf[Double])

  def sumInt(field: String) = sum(field, classOf[Integer])

  def count(outField: String) = new CountBy(outField)

  def first(field: String) = new FirstBy(field)

  def avg(field: String) = new AverageBy(field, field)
}
