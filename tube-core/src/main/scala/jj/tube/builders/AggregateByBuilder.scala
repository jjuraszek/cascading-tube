package jj.tube.builders

import jj.tube._
import cascading.tuple.Fields
import cascading.tuple.Fields._
import scala.language.{reflectiveCalls,existentials}
import cascading.pipe.assembly._

//TODO add policy what to do with nulls
class AggregateByBuilder(val keys:Fields, val baseStream: Tube) extends  OperationBuilder{

  val aggregators = scala.collection.mutable.ListBuffer.empty[AggregateBy]
  var threshold:Option[Int] = None

  private def out(input:Fields,output:Fields) = if(output.isUnknown)input else output

  /** allow in memory aggreagation up to threshold */
  def withThreshold(threshold: Int) = {this.threshold = Some(threshold); this}
  /**
   * create average stat for aggreagation
   * @param input fields with numeric values
   * @param output field with stat result
   */
  def avg(input:Fields, output:Fields = UNKNOWN) = {aggregators += new AverageBy(input, out(input,output)); this}

  /**
   * create sumation stat for aggreagation
   * @param input fields with numeric values
   * @param output field with stat result
   * @tparam T outcome result type (supported Int and Double/Float)
   */
  def sum[T](input:Fields, output:Fields = UNKNOWN)(implicit m: Manifest[T]) = {
    val resultType = if(m.runtimeClass == classOf[Nothing]) classOf[Double] else m.runtimeClass
    aggregators += new SumBy(input, out(input,output), resultType)
    this
  }

  /**
   * create maximum stat for aggreagation
   * @param input fields with numeric values
   * @param output field with stat result
   */
  def max(input:Fields, output:Fields = UNKNOWN) = {aggregators += new MaxBy(input, out(input,output)); this}

  /**
   * create minimum stat for aggreagation
   * @param input fields with numeric values
   * @param output field with stat result
   */
  def min(input:Fields, output:Fields = UNKNOWN) = {aggregators += new MinBy(input, out(input,output)); this}

  /**
   * create count stat for aggreagation
   * @param value fields applied by null policy
   * @param output field with stat result
   * @param nullPolicy describe how to treat nulls in count. default include them.
   */
  def count(value:Fields, output:Fields, nullPolicy: CountBy.Include):AggregateByBuilder = {aggregators += new CountBy(value, output, nullPolicy); this}
  def count(output:Fields):AggregateByBuilder = count(Fields.ALL, output, CountBy.Include.ALL)
  def countNotNull(valueAndOutAlias:Fields) = count(valueAndOutAlias, valueAndOutAlias, CountBy.Include.NO_NULLS)

  /**
   * rewrite first value from input for that aggreagation
   * @param order defining order of values per field
   */
  def first[T<:SortOrder](order: T) = { aggregators += new FirstBy(order.sortedFields); this}

  def go =
    baseStream << threshold
      .map{case th:Int => new AggregateBy(baseStream, keys, th, aggregators: _*)}
      .getOrElse( new AggregateBy(baseStream, keys, aggregators: _*))
}
