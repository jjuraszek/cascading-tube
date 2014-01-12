package jj.tube.builders

import cascading.pipe.{CoGroup, Every, GroupBy}
import jj.tube._
import cascading.tuple.Fields
import cascading.tuple.Fields._
import jj.tube.CustomOps._
import scala.language.reflectiveCalls
import cascading.pipe.assembly._
import java.util.Comparator

//TODO add policy what to do with nulls
class AggregateByBuilder(val keys:Fields, val baseStream: Tube) extends  OperationBuilder{
  val aggregators = scala.collection.mutable.ListBuffer.empty[AggregateBy]

  /**
   * create average stat for aggreagation
   * @param input fields with numeric values
   * @param output field with stat result
   */
  def avg(input:Fields,output:Fields) = {aggregators += new AverageBy(input, output); this}

  /**
   * create sumation stat for aggreagation
   * @param input fields with numeric values
   * @param output field with stat result
   * @tparam T outcome result type (supported Int and Double/Float)
   */
  def sum[T](input:Fields,output:Fields)(implicit m: Manifest[T]) = {
    val resultType = if(m.runtimeClass == classOf[Nothing]) classOf[Double] else m.runtimeClass
    aggregators += new SumBy(input, output, resultType)
    this
  }

  /**
   * create maximum stat for aggreagation
   * @param input fields with numeric values
   * @param output field with stat result
   */
  def max(input:Fields,output:Fields) = {aggregators += new MaxBy(input, output); this}

  /**
   * create minimum stat for aggreagation
   * @param input fields with numeric values
   * @param output field with stat result
   */
  def min(input:Fields,output:Fields) = {aggregators += new MinBy(input, output); this}

  /**
   * create count stat for aggreagation
   * @param output field with stat result
   */
  def count(output:Fields) = {aggregators += new CountBy(output); this}

  /**
   * rewrite first value from input for that aggreagation
   * @param order defining order of values per field
   */
  def first(order: SortOrder) = { aggregators += new FirstBy(order.sortedFields); this}

  def go =
    baseStream <<  new AggregateBy(baseStream, keys, aggregators: _*)
}
