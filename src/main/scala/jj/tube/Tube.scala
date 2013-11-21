package jj.tube

import cascading.pipe._
import cascading.pipe.joiner.{Joiner, InnerJoin}
import cascading.tuple.Fields
import cascading.tuple.Fields._
import cascading.operation.{DebugLevel, Insert, Debug}
import cascading.pipe.assembly._
import cascading.operation.aggregator.First
import CustomOps._
import Tube._
import scala.language.reflectiveCalls
import cascading.operation.buffer.FirstNBuffer

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

class Tube(private var pipe: Pipe) extends GroupOperator with RowOperator with FieldsOperator with MathOperator {
  /**
   * map of flow intersections allowing to dump intermediate data.
   */
  val checkpoints = scala.collection.mutable.Map[String, Checkpoint]()

  /**
   * make checkpoint with name
   *
   * @param name checkpoint name
   * @return tube instance
   */
  def checkpoint(name: Option[String] = None) = this << {
    val chkPoint = new Checkpoint(name.getOrElse(null), pipe)
    if (name.isDefined) checkpoints(name.get) = chkPoint
    chkPoint
  }

  /**
   * order debug output on stream
   * @param level by this param you can turn off debug message on FlowDef
   * @return unaltered tube
   */
  def debug(level: DebugLevel = DebugLevel.VERBOSE) = this << new Each(this, level, new Debug)

  /**
   * Merge this tube with other tubes
   * @param tubes non zero length list of tubes
   * @return current tube with merged tubes
   */
  def merge(tubes: Tube*) = this << new Merge(pipe :: tubes.map(_.pipe).toList: _*)

  /**
   * Split tube for two tubes. One conforming to the {@code filter} and other one not confirming to it
   *
   * @param input fields of closure input
   * @param filter closure predicate intersecting original tube
   * @return tuple of two tubes. First conforming to the filter and second one not confirming to it
   */
  def split(input: Fields = ALL)(filter: Map[String, String] => Boolean) = {
    val positiveTube = Tube("positive_" + pipe.getName, this.pipe)
    positiveTube.filter(input)(!filter(_))

    val negativeTube = Tube("negative_" + pipe.getName, this.pipe)
    negativeTube.filter(input)(filter)
    (positiveTube, negativeTube)
  }

  /**
   * Allow to decorate current tube. Apply transformation to current tube
   * @param op transformation closure received current tube pipe and return transformed pipe
   * @return tube with applied transformation
   */
  def <<(op: Pipe => Pipe): Tube = this << op(this.pipe)

  def <<(op: Pipe): Tube = {
    pipe = op
    this
  }
}

trait GroupOperator {
  this: Tube =>

  /**
   * Make group of this tube
   *
   * @param key groupping keys
   * @param sort sort field of each group (buffer will gets fields in defined order)
   * @param reverse whether to sort in revers order
   * @param bufferScheme closure output fields scheme
   * @param input closure input
   * @param outScheme fields retain after transformation
   * @param buffer closure operating on group fields and rows from each group
   * @return schema from outScheme
   */
  def groupBy(key: Fields, sort: Fields = null, reverse: Boolean = false)
             (bufferScheme: Fields = UNKNOWN, input: Fields = ALL, outScheme: Fields = RESULTS)
             (buffer: (Map[String, String], Iterator[Map[String, String]]) => List[Map[String, Any]]) =
    this << new GroupBy(this, key, sort, reverse) << new Every(this, input, asBuffer(buffer).setOutputScheme(bufferScheme), outScheme)

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
    this << new GroupBy(this, group, sort, descending) << new Every(this, VALUES, new FirstNBuffer(limit))

  /**
   * Join this tube with other big tube.
   *
   * @param leftKey this tube join key
   * @param rightCollection other tube
   * @param rightKey other tube join key
   * @param joiner type of join (inner/outer etc. { @see cascading.pipe.joiner.Joiner})
   * @return key group X this tube scheme X other tube scheme
   */
  def join(leftKey: Fields, rightCollection: Tube, rightKey: Fields, joiner: Joiner = new InnerJoin) =
    this << new CoGroup(this, leftKey, rightCollection, rightKey, joiner)

  /**
   * Join this tube with other big tube. Group the results accordingly to join key and apply operation on each join group
   *
   * @param leftKey this tube join key
   * @param rightCollection other tube
   * @param rightKey other tube join key
   * @param joiner type of join (inner/outer etc. { @see cascading.pipe.joiner.Joiner})
   * @param bufferScheme closure output fields scheme
   * @param input closure input
   * @param outScheme fields retain after transformation
   * @param buffer closure operating on group fields and rows from each group
   * @return schema from outScheme
   */
  def coGroup(leftKey: Fields, rightCollection: Tube, rightKey: Fields, joiner: Joiner = new InnerJoin)
             (bufferScheme: Fields = UNKNOWN, input: Fields = ALL, outScheme: Fields = RESULTS)
             (buffer: (Map[String, String], Iterator[Map[String, String]]) => List[Map[String, Any]]) =
    this << new CoGroup(this, leftKey, rightCollection, rightKey, joiner) << new Every(this, input, asBuffer(buffer).setOutputScheme(bufferScheme), outScheme)

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
    this << new HashJoin(this, leftKey, rightCollection, rightKey, joiner)
}

trait RowOperator {
  this: Tube =>

  /**
   * Make group aggreagtion based on {@code key} and performing accumulation defined in {@code aggregators} for each group.
   *
   * @param key groupping fields
   * @param aggregators accumulators
   * @return transformed tube with scheme containing group and effect of accumulation
   */
  //TODO transform to builder pattern
  def aggregateBy(key: Fields, aggregators: AggregateBy*) = this << new AggregateBy(this, key, aggregators: _*)

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
  //TODO operate on serialized value (ex. Json)
  def each(input: Fields = ALL, funcScheme: Fields = UNKNOWN, outScheme: Fields = ALL)
          (function: (Map[String, String] => Map[String, Any])) =
    this << new Each(this, input, asFunction(function).setOutputScheme(funcScheme), outScheme)

  /**
   * Allow replace fields in input row
   * @param input fields to replace
   * @param function transforming closure
   * @return row scheme. values from input replaced
   */
  def replace(input: Fields, funcScheme: Fields = ARGS)
             (function: (Map[String, String] => Map[String, Any])) =
    if(funcScheme == ARGS) each(input, funcScheme, REPLACE)(function)
    else each(input, funcScheme)(function).discard(input)

  /**
   * Filtering this tube according to defined closure
   *
   * @param input fields of closure input
   * @param filter closure predicate. If true rule out the row
   * @return fields are not altered. Only row count is different
   */
  def filter(input: Fields = ALL)(filter: Map[String, String] => Boolean) = this << new Each(this, input, asFilter(filter))

  /**
   * Delete duplicates from this tube
   *
   * @param fields fields defining uniqueness
   * @return fields are not altered. Only unique rows
   */
  def unique(fields: Fields = ALL) = this << new Unique(this, fields)
}

trait FieldsOperator {
  this: Tube =>

  /**
   * Discard fields
   *
   * @param field fields to remove from tube
   * @return original fields - removed fields
   */
  def discard(field: Fields) = this << new Discard(this, field)

  /**
   * Rename some fields
   * @param from fields to rename
   * @param to new names (quantity of fields name must be equal to 'from')
   * @return original fields - from + to
   */
  def rename(from: Fields, to: Fields) = this << new Rename(this, from, to)

  /**
   * Keep fields listed in {@code fields}
   *
   * @param fields
   * @return fields from param 'fields'
   */
  def retain(fields: Fields) = this << new Retain(this, fields)

  /**
   * Alter type of fields.
   * @param fields fields for transformation
   * @param klass type of destination class
   * @return same as input fields. Only values may be altered (ie. Double -> Integer)
   */
  def coerce(fields: Fields, klass: Class[_]) = this << new Coerce(this, fields, (1 to fields.size).map(_ => klass): _*)

  /**
   * Append some constances to tube
   *
   * @param field name of appended constances
   * @param value values for each new field
   * @return input fields + 'fields' from param
   */
  def insert(field: Fields, value: String*) = this << new Each(this, new Insert(field, value: _*), ALL)
}

trait MathOperator {
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
  def math(leftOp: String, rightOp: String, outField: String)(func: (Double, Double) => Double) =
    each((leftOp, rightOp), outField) {
      row =>
        Map(outField -> func(row(leftOp).toDouble, row(rightOp).toDouble))
    }

  /**
   * Math operation with single operand. {@code operand} must be convertable to Double.
   */
  def math(operand: String, outField: String)(func: Double => Double) =
    each(operand, outField) {
      row =>
        Map(outField -> func(row(operand).toDouble))
    }
}
