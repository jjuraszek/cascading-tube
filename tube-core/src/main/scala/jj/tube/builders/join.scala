

package jj.tube.builders


import jj.tube._
import cascading.tuple.{TupleEntry, Fields}
import cascading.pipe.joiner.{BufferJoin, Joiner, InnerJoin}
import cascading.pipe.{Every, HashJoin, CoGroup}
import cascading.operation.{BufferCall, Buffer, BaseOperation}
import cascading.flow.FlowProcess
import scala.collection.convert.WrapAsScala.asScalaIterator
import scala.language.{reflectiveCalls, existentials}
import jj.tube.util.TupleEntriesIterator

trait WithJoinStrategy[T] {
  this: T =>

  var joinStrategy: Joiner = new InnerJoin

  /**
   * set up inner, outter or other custom strategy
   */
  def withJoinStrategy(joinStrategy: Joiner) = {
    this.joinStrategy = joinStrategy
    this
  }
}

trait JoinApply[T] extends WithOperationResult[T] {
  this: T =>

  var leftStreamKey: Fields = _
  var rightStreamKey: Fields = _

  /**
   * @param keys set join key same for both sides
   */
  def on(keys: Fields) = {
    leftStreamKey = keys
    rightStreamKey = keys
    this
  }

  /**
   * set join key potentially different for both sides
   */
  def on(leftKey: Fields, rightKey: Fields) = {
    leftStreamKey = leftKey
    rightStreamKey = rightKey
    this
  }
}

class JoinBuilder(val leftStream: Tube, val rightStream: Tube) extends OperationBuilder
with JoinApply[JoinBuilder]
with WithJoinStrategy[JoinBuilder] {

  def go =
    leftStream << new CoGroup(leftStream, leftStreamKey, rightStream, rightStreamKey, declaringScheme, resultScheme, joinStrategy)
}

class HashJoinBuilder(val leftStream: Tube, val rightStream: Tube) extends OperationBuilder
with JoinApply[HashJoinBuilder]
with WithJoinStrategy[HashJoinBuilder] {

  def go =
    leftStream << new HashJoin(leftStream, leftStreamKey, rightStream, rightStreamKey, declaringScheme, joinStrategy)
}

class CustomJoinBuilder(val leftStream: Tube, val rightStream: Tube) extends OperationBuilder
with JoinApply[CustomJoinBuilder]
with WithCustomOperation[CustomJoinBuilder, JOIN] {

  withInput(Fields.ALL)
  withResult(Fields.RESULTS)

  def go =
    leftStream << {
      new CoGroup(leftStream, leftStreamKey, rightStream, rightStreamKey, new BufferJoin)
    } << new Every(leftStream, inputScheme, asCustomJoin(operation).setOutputScheme(declaringScheme), resultScheme)

  private def asCustomJoin(transform: JOIN) =
    new BaseOperation[Any] with Buffer[Any] {
      override def operate(flowProcess: FlowProcess[_], bufferCall: BufferCall[Any]) {
        val joiner = bufferCall.getJoinerClosure
        transform(
          TupleEntriesIterator(joiner.getIterator(0), joiner.getValueFields()(0), bufferCall.getOutputCollector),
          builIterable(bufferCall, 1))
          .foreach(WithCustomOperation.writeTupleEntryToOutput(_, bufferCall.getOutputCollector))
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }

  private def builIterable(bufferCall: BufferCall[Any], iteratorIndex: Int) = new Iterable[TupleEntry] {
    override def iterator =
      TupleEntriesIterator(
        bufferCall.getJoinerClosure.getIterator(iteratorIndex),
        bufferCall.getJoinerClosure.getValueFields()(iteratorIndex),
        bufferCall.getOutputCollector)
  }
}

