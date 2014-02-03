package jj.tube.alterations

import cascading.tuple.Fields
import jj.tube.builders._
import cascading.pipe.{Every, GroupBy}
import cascading.tuple.Fields._
import cascading.operation.buffer.FirstNBuffer
import jj.tube.Tube

trait GroupOperator {
  this: Tube =>

  /**
   * @return init grouping builder
   */
  def groupBy(keyGroup: Fields) = new GroupingBuilder(this).on(keyGroup)

  /**
   * Take top n rows from each group
   *
   * @param group fields defining group
   * @param sort sorting fields in each group
   * @param descending whether to sort group in descending order
   * @param limit how many rows to keep from each group
   * @return rows fields are not altered. Only row count is different
   */
  def top(group: Fields, sort: Fields, descending: Boolean = false, limit: Int = 1) =
    this << new GroupBy(this, group, sort, descending) << new Every(this, ALL, new FirstNBuffer(limit), ARGS)

  /**
   * @param rightCollection right collection for joining
   * @return gets the join builder of this tube and the second tube to join it with
   */
  def join(rightCollection: Tube) = new JoinBuilder(this, rightCollection)

  /**
   * join with other collection and apply operation on each join group
   */
  def coGroup(rightCollection:Tube) = new CoGroupingBuilder(this,rightCollection)

  /**
   * allow joining on two streams with specific keys and custom predicates and pre-filling
   */
  def customJoin(rightCollection:Tube) = new CustomJoinBuilder(this,rightCollection)

  /**
   * @param rightCollection right collection for joining fitting mapper memory
   * @return gets the join builder of this tube and the second tube to join it with
   */
  def hashJoin(rightCollection: Tube) = new HashJoinBuilder(this, rightCollection)
}
