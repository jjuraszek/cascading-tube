package jj.tube.alterations

import cascading.tuple.Fields
import jj.tube.builders.{MapBuilder, FilterBuilder, FlatMapBuilder, AggregateByBuilder}
import cascading.tuple.Fields._
import jj.tube._
import cascading.pipe.assembly.Unique

trait RowOperator {
  this: Tube =>

  /**
   * @param key key used as group by aggreagation
   * @return builder for aggreagtion
   */
  def aggregateBy(key: Fields) = new AggregateByBuilder(key, this)

  /**
   * allow extract values for each row
   * @param input input to each operation
   * @return builder of each operator
   */
  def flatMap(input: Fields = ALL) = new FlatMapBuilder(this).withInput(input).withResult(ALL)

  /**
   * transform each row according to surjective function
   * @param input input to each operation
   * @return builder of each operator
   */
  def map(input: Fields = ALL) = new MapBuilder(this).withInput(input).withResult(ALL)

  /**
   * Replace fields with another values
   * @param input fields to replace
   * @return only input fields are altered in that transformation
   */
  def replace(input: Fields) = new FlatMapBuilder(this).withInput(input).declaring(ARGS).withResult(REPLACE)

  /**
   * Filtering this tube according to defined closure
   *
   * @param filter closure predicate. If true rule out the row
   * @return fields are not altered. Only row count is different
   */
  def filter(filter:FILTER) = new FilterBuilder(this)(!filter(_))

  /**
   *
   * @see filter working as opposite filter
   */
  def filterNot(filter:FILTER) = new FilterBuilder(this)(filter)

  /**
   * Delete duplicates from this tube
   *
   * @param fields fields defining uniqueness
   * @return fields are not altered. Only unique rows
   */
  def unique(fields: Fields = ALL) = this << new Unique(this, fields)
}
