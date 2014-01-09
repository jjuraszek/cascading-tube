

package jj.tube.builders


import jj.tube.Tube
import cascading.tuple.Fields
import cascading.pipe.joiner.{Joiner, InnerJoin}
import cascading.pipe.{HashJoin, CoGroup}

class JoinBuilder(val leftStream: Tube, val rightStream: Tube) extends OperationBuilder{
  var joinerImpl:Joiner = new InnerJoin()

  var leftStreamKey:Fields = _
  var rightStreamKey:Fields = _
  var outputScheme:Fields = _

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

  /**
   * @return apply join and return resulting Tube
   */
  def go =
    leftStream << new CoGroup(leftStream, leftStreamKey, rightStream, rightStreamKey, outputScheme, joinerImpl)
}

class HashJoinBuilder(leftStream: Tube, rightStream: Tube) extends JoinBuilder(leftStream, rightStream){
  override def go =
    leftStream << new HashJoin(leftStream, leftStreamKey, rightStream, rightStreamKey, outputScheme, joinerImpl)
}
