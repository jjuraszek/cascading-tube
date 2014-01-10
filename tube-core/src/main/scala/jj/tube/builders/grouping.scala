package jj.tube.builders

import cascading.pipe.{CoGroup, Every, GroupBy}
import jj.tube._
import cascading.tuple.Fields
import cascading.tuple.Fields._
import jj.tube.CustomOps._
import scala.language.reflectiveCalls

trait BufferApply[T] extends OperationBuilder{ this: T =>
  val baseStream: Tube
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

  protected def every = new Every(baseStream, inputFields, asBuffer(buffer).setOutputScheme(bufferScheme), resultScheme)
}

class CoGroupingBuilder(val baseStream: Tube, val rightStream: Tube) extends JoinApply[CoGroupingBuilder] with BufferApply[CoGroupingBuilder] {
  def go =
    baseStream <<  new CoGroup(baseStream, leftStreamKey, rightStream, rightStreamKey, outputScheme, joinerImpl) << every

  /**
   * imply sort of input for each transformation according to fields and direction
   */
  override def sorted(sort: Fields, order: Order): CoGroupingBuilder =
    throw new NotImplementedError("cascading is not supporting sort for coGroup; instead use join and group by")
}

class GroupingBuilder(val keys: Fields, val baseStream: Tube) extends BufferApply[GroupingBuilder] {
  def go =
    baseStream << new GroupBy(baseStream, keys, sort, order.dir) << every
}
