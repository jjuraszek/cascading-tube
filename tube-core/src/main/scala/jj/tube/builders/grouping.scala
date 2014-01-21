package jj.tube.builders

import cascading.pipe.{CoGroup, Every, GroupBy}
import jj.tube._
import cascading.tuple.Fields
import cascading.tuple.Fields._
import scala.language.{reflectiveCalls,existentials}
import cascading.operation.{BufferCall, Buffer, BaseOperation}
import cascading.flow.FlowProcess
import scala.collection.convert.WrapAsScala.asScalaIterator
import jj.tube.SortOrder
import jj.tube.RichTupleEntry

trait BaseGroupingBuilder extends OperationBuilder{
  val baseStream:Tube
  var input:Fields
  var operation: (RichTupleEntry, Iterator[RichTupleEntry]) => List[RichTupleEntry]
  var operationScheme:Fields
  var resultScheme:Fields

  def every = new Every(baseStream, input, asBuffer(operation).setOutputScheme(operationScheme), resultScheme)

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

class GroupingBuilder(val baseStream: Tube) extends BaseGroupingBuilder
  with WithCustomOperation[GroupingBuilder,(RichTupleEntry, Iterator[RichTupleEntry]) => List[RichTupleEntry]]
  with WithOperationResult[GroupingBuilder]{

  withInput(ALL)
  declaring(UNKNOWN)
  withResult(RESULTS)

  var order = SortOrder(UNKNOWN)
  var keys:Fields = null

  def on(keys:Fields) = {this.keys = keys; this}

  /**
   * imply sort of input for each transformation according to fields and direction
   */
  def sorted(order: SortOrder) = {
    this.order = order
    this
  }

  def go =
    baseStream << new GroupBy(baseStream, keys, order.sortedFields, order.reverse) << every
}

class CoGroupingBuilder(val baseStream: Tube, val rightStream: Tube) extends BaseGroupingBuilder
  with JoinApply[CoGroupingBuilder]
  with WithCustomOperation[CoGroupingBuilder,(RichTupleEntry, Iterator[RichTupleEntry]) => List[RichTupleEntry]]
  with WithOperationResult[CoGroupingBuilder]{

  var joinFields:Fields = null

  def withJoinFields(alter:Fields) = { this.joinFields = alter; this}

  withInput(ALL)
  declaring(UNKNOWN)
  withResult(RESULTS)

  override def on(keys:Fields) = {
    leftStreamKey = keys
    rightStreamKey = keys
    this
  }

  override def go =
    baseStream << {
      new CoGroup(baseStream, leftStreamKey, rightStream, rightStreamKey, joinFields, joinerImpl)
    } << every
}
