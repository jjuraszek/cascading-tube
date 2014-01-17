package jj.tube.builders

import cascading.pipe.{CoGroup, Every, GroupBy}
import jj.tube._
import cascading.tuple.Fields
import cascading.tuple.Fields._
import scala.language.reflectiveCalls
import cascading.operation.{BufferCall, Buffer, BaseOperation}
import cascading.flow.FlowProcess
import scala.collection.convert.WrapAsScala.asScalaIterator

trait BufferApply[T] extends OperationBuilder{ this: T =>
  val baseStream: Tube
  var buffer: (RichTupleEntry, Iterator[RichTupleEntry]) => List[RichTupleEntry] = _
  var order = SortOrder(UNKNOWN)
  var inputFields = ALL
  var bufferScheme = UNKNOWN
  var resultScheme = RESULTS

  /**
   * pass transformation function
   */
  def map(buffer: (RichTupleEntry, Iterator[RichTupleEntry]) => List[RichTupleEntry]) = {
    this.buffer = buffer
    this
  }

  /**
   * imply sort of input for each transformation according to fields and direction
   */
  def sorted(order: SortOrder) = {
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

  def asBuffer(transform: (RichTupleEntry, Iterator[RichTupleEntry]) => List[RichTupleEntry]) =
    new BaseOperation[Any] with Buffer[Any] {
      override def operate(flowProcess: FlowProcess[_], bufferCall: BufferCall[Any]) {
        val lazyIterator = bufferCall.getArgumentsIterator.map(new RichTupleEntry(_))
        transform(bufferCall.getGroup, lazyIterator)
          .map(_.tupleEntry)
          .foreach(bufferCall.getOutputCollector.add)
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }
}

class CoGroupingBuilder(val baseStream: Tube, val rightStream: Tube) extends JoinApply[CoGroupingBuilder] with BufferApply[CoGroupingBuilder] {
  def go =
    baseStream <<  new CoGroup(baseStream, leftStreamKey, rightStream, rightStreamKey, outputScheme, joinerImpl) << every

  /**
   * imply sort of input for each transformation according to fields and direction
   */
  override def sorted(order: SortOrder): CoGroupingBuilder =
    throw new NotImplementedError("cascading is not supporting sort for coGroup; instead use join and group by")
}

class GroupingBuilder(val keys: Fields, val baseStream: Tube) extends BufferApply[GroupingBuilder] {
  def go =
    baseStream << new GroupBy(baseStream, keys, order.sortedFields, order.reverse) << every
}
