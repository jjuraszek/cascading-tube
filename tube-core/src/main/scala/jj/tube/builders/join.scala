

package jj.tube.builders


import jj.tube._
import cascading.tuple.{TupleEntry, Fields}
import cascading.pipe.joiner.{BufferJoin, Joiner, InnerJoin}
import cascading.pipe.{Every, HashJoin, CoGroup}
import cascading.operation.{BufferCall, Buffer, BaseOperation}
import cascading.flow.FlowProcess
import scala.collection.convert.WrapAsScala.asScalaIterator
import scala.language.{reflectiveCalls,existentials}

trait WithJoinStrategy[T] extends OperationBuilder{ this: T =>

  var joinerImpl:Joiner = new InnerJoin

  /**
   * set up inner, outter or other custom strategy
   */
  def withJoinStrategy(joiner: Joiner) = {
    joinerImpl = joiner
    this
  }
}

trait JoinApply[T] extends OperationBuilder
  with WithOperationResult[T]{ this: T =>

  var leftStreamKey:Fields = _
  var rightStreamKey:Fields = _

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
  def on(leftKey:Fields, rightKey:Fields) = {
    leftStreamKey = leftKey
    rightStreamKey = rightKey
    this
  }
}

class JoinBuilder(val leftStream: Tube, val rightStream: Tube) extends JoinApply[JoinBuilder] with WithJoinStrategy[JoinBuilder]{
  def go =
    leftStream << new CoGroup(leftStream, leftStreamKey, rightStream, rightStreamKey, operationScheme, resultScheme, joinerImpl)
}

class HashJoinBuilder(val leftStream: Tube, val rightStream: Tube) extends JoinApply[HashJoinBuilder] with WithJoinStrategy[HashJoinBuilder] {
  def go =
    leftStream << new HashJoin(leftStream, leftStreamKey, rightStream, rightStreamKey, operationScheme, joinerImpl)
}

class CustomJoinBuilder(val leftStream: Tube, val rightStream: Tube) extends JoinApply[CustomJoinBuilder]
  with WithCustomOperation[CustomJoinBuilder,JOIN]{

  withInput(Fields.ALL)
  withResult(Fields.RESULTS)

  def go =
    leftStream << {
      new CoGroup(leftStream, leftStreamKey, rightStream, rightStreamKey, new BufferJoin)
    } << new Every(leftStream, input, asCustomJoin(operation).setOutputScheme(operationScheme), resultScheme)

  private def asCustomJoin(transform: JOIN) =
    new BaseOperation[Any] with Buffer[Any] {
      override def operate(flowProcess: FlowProcess[_], bufferCall: BufferCall[Any]) {
        val joiner = bufferCall.getJoinerClosure
        val leftStream = joiner.getIterator(0).map(new TupleEntry(joiner.getValueFields()(0),_))
        val rightStream = joiner.getIterator(1).map(new TupleEntry(joiner.getValueFields()(1),_))
        transform(leftStream, rightStream)
          .foreach(bufferCall.getOutputCollector.add)
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }
}

