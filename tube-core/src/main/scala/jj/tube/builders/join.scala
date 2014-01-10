

package jj.tube.builders


import jj.tube.Tube
import cascading.tuple.Fields
import cascading.pipe.joiner.{Joiner, InnerJoin}
import cascading.pipe.{HashJoin, CoGroup}

trait JoinApply[T] extends OperationBuilder{ this: T =>
  var joinerImpl:Joiner = new InnerJoin

  var leftStreamKey:Fields = _
  var rightStreamKey:Fields = _
  var outputScheme:Fields = _

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

  /**
   * set up inner, outter or other custom strategy
   */
  def withJoinStrategy(joiner: Joiner) = {
    joinerImpl = joiner
    this
  }

  /**
   * set up output scheme after join
   * @param outputScheme aliases of fields are assigned to output from all left stream fields and then right stream fields in order
   */
  def withOutputScheme(outputScheme: Fields) = {
    this.outputScheme = outputScheme
    this
  }
}

class JoinBuilder(val leftStream: Tube, val rightStream: Tube) extends JoinApply[JoinBuilder] {
  def go =
    leftStream << new CoGroup(leftStream, leftStreamKey, rightStream, rightStreamKey, outputScheme, joinerImpl)
}

class HashJoinBuilder(val leftStream: Tube, val rightStream: Tube) extends JoinApply[HashJoinBuilder] {
  def go =
    leftStream << new HashJoin(leftStream, leftStreamKey, rightStream, rightStreamKey, outputScheme, joinerImpl)
}
