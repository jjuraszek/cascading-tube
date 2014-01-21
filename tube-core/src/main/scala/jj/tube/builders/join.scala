

package jj.tube.builders


import jj.tube.Tube
import cascading.tuple.Fields
import cascading.pipe.joiner.{Joiner, InnerJoin}
import cascading.pipe.{HashJoin, CoGroup}

trait JoinApply[T] extends OperationBuilder
  with WithOperationResult[T]{ this: T =>
  var joinerImpl:Joiner = new InnerJoin

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

  /**
   * set up inner, outter or other custom strategy
   */
  def withJoinStrategy(joiner: Joiner) = {
    joinerImpl = joiner
    this
  }
}

class JoinBuilder(val leftStream: Tube, val rightStream: Tube) extends JoinApply[JoinBuilder]{
  def go =
    leftStream << new CoGroup(leftStream, leftStreamKey, rightStream, rightStreamKey, operationScheme, resultScheme, joinerImpl)
}

class HashJoinBuilder(val leftStream: Tube, val rightStream: Tube) extends JoinApply[HashJoinBuilder] {
  def go =
    leftStream << new HashJoin(leftStream, leftStreamKey, rightStream, rightStreamKey, operationScheme, joinerImpl)
}
