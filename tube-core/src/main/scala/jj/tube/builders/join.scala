package jj.tube.builders

import jj.tube.Tube
import cascading.tuple.Fields
import cascading.pipe.joiner.{Joiner, InnerJoin}
import cascading.pipe.{HashJoin, CoGroup}

class JoinBuilder(val leftStream: Tube, val rightStream: Tube) {
  var joinerImpl:Joiner = new InnerJoin()

  var leftStreamKey:Fields = _
  var rightStreamKey:Fields = _

  /**
   * @param keys set join key same for both sides
   */
  def on(keys: Fields):JoinBuilder = on(keys, keys)

  /**
   * set join key potentially different for both sides
   */
  def on(leftKey:Fields, rightKey:Fields) = {
    leftStreamKey = leftKey
    rightStreamKey = rightKey
    this
  }

  def withJoinStrategy(joiner: Joiner) = {
    joinerImpl = joiner
    this
  }

  /**
   * apply join
   * @param outputScheme result schema (aliases of fields from left tube and then from right tube)
   * @return Tube result
   */
  def getting(outputScheme: Fields = null) = leftStream << new CoGroup(leftStream, leftStreamKey, rightStream, rightStreamKey, outputScheme, joinerImpl)
}

class HashJoinBuilder(leftStream: Tube, rightStream: Tube) extends JoinBuilder(leftStream, rightStream){
  override def getting(outputScheme: Fields = null) = leftStream << new HashJoin(leftStream, leftStreamKey, rightStream, rightStreamKey, outputScheme, joinerImpl)
}
