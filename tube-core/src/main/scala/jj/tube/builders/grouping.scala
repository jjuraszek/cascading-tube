package jj.tube.builders

import cascading.pipe.{Every, GroupBy}
import jj.tube._
import cascading.tuple.Fields
import cascading.tuple.Fields._
import jj.tube.CustomOps._
import scala.language.reflectiveCalls

class CoGroupingBuilder(leftStream: Tube, rightStream: Tube) extends JoinBuilder(leftStream, rightStream) {
  override def go = throw new NotImplementedError()
}

class GroupingBuilder(val keys: Fields, var stream: Tube) extends OperationBuilder {
  var buffer: (Map[String, String], Iterator[Map[String, String]]) => List[Map[String, Any]] = _
  var sort: Fields = null
  var order = ASC
  var inputFields = ALL
  var bufferScheme = UNKNOWN
  var resultScheme = RESULTS

  /**
   * pass transformation function
   */
  def map(buffer: (Map[String, String], Iterator[Map[String, String]]) => List[Map[String, Any]]) = {
    this.buffer = buffer
    this
  }

  /**
   * imply sort of input for each transformation according to fields and direction
   */
  def sorted(sort: Fields, order: Order = ASC) = {
    this.sort = sort
    this.order = order
    this
  }

  /**
    * define input of transformation
   */
  def withInput(fields: Fields) = {
    this.inputFields = fields
    this
  }

  /**
    * declare transformation output
   */
  def declaring(scheme: Fields) = {
    this.bufferScheme = scheme
    this
  }

  /**
    * declare final output schema for grouping operation
   */
  def withResult(scheme: Fields) = {
    this.resultScheme = scheme
    this
  }

  def go: Tube =
    stream << new GroupBy(stream, keys, sort, order.dir) <<
      new Every(stream, inputFields, asBuffer(buffer).setOutputScheme(bufferScheme), resultScheme)
}
