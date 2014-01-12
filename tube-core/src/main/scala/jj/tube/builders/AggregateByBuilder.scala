package jj.tube.builders

import cascading.pipe.{CoGroup, Every, GroupBy}
import jj.tube._
import cascading.tuple.Fields
import cascading.tuple.Fields._
import jj.tube.CustomOps._
import scala.language.reflectiveCalls
import cascading.pipe.assembly._
import java.util.Comparator


class AggregateByBuilder(val keys:Fields, val baseStream: Tube) extends  OperationBuilder{
  val aggregators = scala.collection.mutable.ListBuffer.empty[AggregateBy]

  def avg(input:Fields,output:Fields) = {aggregators += new AverageBy(input, output); this}
  def sum[T](input:Fields,output:Fields)(implicit m: Manifest[T]) = {
    val resultType = if(m.runtimeClass == classOf[Nothing]) classOf[Double] else m.runtimeClass
    aggregators += new SumBy(input, output, resultType)
    this
  }
  def max(input:Fields,output:Fields) = {aggregators += new MaxBy(input, output); this}
  def min(input:Fields,output:Fields) = {aggregators += new MinBy(input, output); this}
  def count(output:Fields) = {aggregators += new CountBy(output); this}
  def first(order: Order) = { aggregators += new FirstBy(order.sortedFields); this}

  def go =
    baseStream <<  new AggregateBy(baseStream, keys, aggregators: _*)
}
