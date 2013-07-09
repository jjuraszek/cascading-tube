package jj.tube

import cascading.pipe._
import cascading.pipe.joiner.{Joiner, InnerJoin}
import cascading.tuple.Fields
import cascading.tuple.Fields._
import cascading.operation.Insert
import cascading.pipe.assembly._
import cascading.operation.aggregator.First
import CustomOps._
import Tube._

class Tube(var pipe: Pipe) extends Grouping with GroupOperator with RowOperator with FieldsTransform with MathOperation {
  def checkpoint = this << new Checkpoint(pipe)

  def merge(tubes: Tube*) = this << new Merge(pipe :: tubes.map(_.pipe).toList: _*)

  def <<(op: Pipe) = {
    pipe = op
    this
  }
}

/**
 * Companion object creating tube and containing implicit conversions allowing usage of tube requiring pipe and pipe to tube.
 */
object Tube {
  /**
   * Creating brand new tube with brand new pipe.
   *
   * @param name pipe name
   * @return tube instance
   */
  def apply(name: String) = new Tube(new Pipe(name))

  /**
   * Creating tube based on previous pipe
   *
   * @param name new pipe name
   * @param previous previouse pipe
   * @return tube instance
   */
  def apply(name: String, previous: Pipe) = new Tube(new Pipe(name, previous))

  implicit def toPipe(tube: Tube) = tube.pipe

  implicit def toTube(pipe: Pipe) = new Tube(pipe)
}

trait Grouping {
  this: Tube =>

  /**
   * Make group aggreagtion based on {@code key} and performing accumulation defined in {@code aggregators} for each group.
   *
   * @param key groupping fields
   * @param aggregators accumulators
   * @return transformed tube with scheme containing group and effect of accumulation
   */
  //TODO transform to builder pattern
  def aggregateBy(key: Fields, aggregators: AggregateBy*) = this << new AggregateBy(pipe, key, aggregators: _*)

  /**
   * Make group of this tube
   *
   * @param key groupping keys
   * @param sort sort field of each group (buffer will gets fields in defined order)
   * @param reverse whether to sort in revers order
   * @return schema containing: group X set(rows)
   */
  //TODO incorporate every in that op
  def groupBy(key: Fields, sort: Fields, reverse: Boolean = false) = this << new GroupBy(pipe, key, sort, reverse)

  /**
   * Join this tube with other big tube.
   *
   * @param leftKey this tube join key
   * @param rightCollection other tube
   * @param rightKey other tube join key
   * @param joiner type of join (inner/outer etc. { @see cascading.pipe.joiner.Joiner})
   * @return key group X this tube scheme X other tube scheme
   */
  def coGroup(leftKey: Fields, rightCollection: Tube, rightKey: Fields, joiner: Joiner = new InnerJoin) =
    this << new CoGroup(pipe, leftKey, rightCollection, rightKey, joiner)

  /**
   * Join this tube with other tube fit to be in memory.
   *
   * @param leftKey this tube join key
   * @param rightCollection other tube
   * @param rightKey other tube join key
   * @param joiner type of join (inner/outer etc. { @see cascading.pipe.joiner.Joiner})
   * @return key group X this tube scheme X other tube scheme
   */
  def hashJoin(leftKey: Fields, rightCollection: Tube, rightKey: Fields, joiner: Joiner = new InnerJoin) =
    this << new HashJoin(pipe, leftKey, rightCollection, rightKey, joiner)
}

//TODO method replace
trait RowOperator {
  this: Tube =>

  /**
   * Transformation allowing to run closure against each row
   *
   * @param input fields of closure input
   * @param funcScheme closure output scheme
   * @param outScheme fields retain after transformation
   * @param function transforming closure
   * @return fields of outScheme
   */
  //TODO each supporting List => List
  def each(input: Fields = ALL, funcScheme: Fields = UNKNOWN, outScheme: Fields = ALL)
          (function: (Map[String, String] => Map[String, Any])) =
    this << new Each(pipe, input, asFunction(function).setOutputScheme(funcScheme), outScheme)

  /**
   * Filtering this tube according to defined closure
   *
   * @param input fields of closure input
   * @param filter closure predicate. If true rule out the row
   * @return fields are not altered. Only row count is different
   */
  def filter(input: Fields = ALL)(filter: Map[String, String] => Boolean) = this << new Each(pipe, input, asFilter(filter))

  /**
   * Delete duplicates from this tube
   *
   * @param fields fields defining uniqueness
   * @return fields are not altered. Only unique rows
   */
  def unique(fields: Fields = ALL) = this << new Unique(pipe, fields)
}

trait GroupOperator {
  this: Tube =>

  /**
   * Transform grouped rows. Operate with closure on each group
   *
   * @param input closure input
   * @param bufferScheme closure output fields
   * @param outScheme fields retain after transformation
   * @param buffer closure operating on group fields and rows from each group
   * @return fields from outScheme
   */
  def every(input: Fields = ALL, bufferScheme: Fields = UNKNOWN, outScheme: Fields = RESULTS)
           (buffer: (Map[String, String], Iterator[Map[String, String]]) => List[Map[String, Any]]) =
    this << new Every(pipe, input, asBuffer(buffer).setOutputScheme(bufferScheme), outScheme)

  /**
   * Take top n rows from each group
   *
   * @param group fields defining group
   * @param sort sorting fields in each group
   * @param reverse whether to sort group in reverse order
   * @param limit how many rows to keep from each group
   * @return rows fields are not altered. Only row count is different
   */
  def top(group: Fields, sort: Fields, reverse: Boolean = false, limit: Int = 1) = {
    groupBy(group, sort, reverse)
    this << new Every(pipe, VALUES, new First(limit))
  }
}

//TODO append
trait FieldsTransform {
  this: Tube =>

  /**
   * Discarde fields
   *
   * @param field fields to remove from tube
   * @return original fields - removed fields
   */
  def discard(field: Fields) = this << new Discard(pipe, field)

  /**
   * Rename some fields
   * @param from fields to rename
   * @param to new names (quantity of fields name must be equal to 'from')
   * @return original fields - from + to
   */
  def rename(from: Fields, to: Fields) = this << new Rename(pipe, from, to)

  /**
   * Keep fields listed in {@code fields}
   *
   * @param fields
   * @return fields from param 'fields'
   */
  def retain(fields: Fields) = this << new Retain(pipe, fields)

  /**
   * Alter type of fields.
   * @param fields fields for transformation
   * @param klass type of destination class
   * @return same as input fields. Only values may be altered (ie. Double -> Integer)
   */
  def coerce(fields: Fields, klass: Class[_]) = this << new Coerce(pipe, fields, (1 to fields.size).map(_ => klass): _*)

  /**
   * Append some constances to tube
   *
   * @param field name of appended constances
   * @param value values for each new field
   * @return input fields + 'fields' from param
   */
  def insert(field: Fields, value: String*) = this << new Each(pipe, new Insert(field, value: _*), ALL)
}

trait MathOperation {
  this: Tube =>

  /**
   * Divide value from {@code leftOp} by value from {@code rightOp} and store result in {@code outField}. {@code leftOp} and {@code rightOp} must be convertable to Double.
   */
  def divide(leftOp: String, rightOp: String, outField: String) = math(leftOp, rightOp, outField) {
    (a, b) => a / b
  }

  /**
   * Multiply value {@code leftOp} and {@code rightOp} and store result in {@code outField}. {@code leftOp} and {@code rightOp} must be convertable to Double.
   */
  def multiply(leftOp: String, rightOp: String, outField: String) = math(leftOp, rightOp, outField) {
    (a, b) => a * b
  }

  /**
   * Add value {@code leftOp} and {@code rightOp} and store result in {@code outField}. {@code leftOp} and {@code rightOp} must be convertable to Double.
   */
  def plus(leftOp: String, rightOp: String, outField: String) = math(leftOp, rightOp, outField) {
    (a, b) => a + b
  }

  /**
   * Minus {@code rightOp} from {@code leftOp} and store result in {@code outField}. {@code leftOp} and {@code rightOp} must be convertable to Double.
   */
  def minus(leftOp: String, rightOp: String, outField: String) = math(leftOp, rightOp, outField) {
    (a, b) => a - b
  }

  /**
   * Math operation with left and right operand. {@code leftOp} and {@code rightOp} must be convertable to Double.
   */
  def math(leftOp: String, rightOp: String, outField: String)(func: (Double, Double) => Double) = {
    this << each((leftOp, rightOp), outField) {
      row =>
        Map(outField -> func(row(leftOp).toDouble, row(rightOp).toDouble))
    }
  }

  /**
   * Math operation with single operand. {@code operand} must be convertable to Double.
   */
  def math(operand: String, outField: String)(func: Double => Double) = {
    this << each(operand, outField) {
      row =>
        Map(outField -> func(row(operand).toDouble))
    }
  }
}
