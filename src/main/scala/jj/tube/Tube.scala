package jj.tube

import cascading.pipe._
import cascading.pipe.joiner.{Joiner, InnerJoin}
import cascading.tuple.Fields
import cascading.operation.{Insert, Filter, Buffer}
import cascading.pipe.assembly._
import cascading.operation.aggregator.First
import cascading.operation.expression.ExpressionFunction

object Tube {
  def apply(name: String) = new Tube(new Pipe(name))

  def apply(name: String, previous: Pipe) = new Tube(new Pipe(name, previous))

  implicit def toPipe(tube: Tube) = tube.pipe

  implicit def toTube(pipe: Pipe) = new Tube(pipe)
}

class Tube(var pipe: Pipe) extends Grouping with GroupOperator with RowOperator with FieldsTransform with MathOperation {
  def checkpoint = this << new Checkpoint(pipe)

  def <<(op: Pipe) = {
    pipe = op
    this
  }
}

trait Grouping {
  this: Tube =>

  def aggregateBy(key: Fields, aggregators: AggregateBy*) = this << new AggregateBy(pipe, key, aggregators: _*)

  def groupBy(key: Fields, sort: Fields, reverse: Boolean = false) = this << new GroupBy(pipe, key, sort, reverse)

  def coGroup(leftKey: Fields, rightCollection: Tube, rightKey: Fields, joiner: Joiner = new InnerJoin) = this << new CoGroup(pipe, leftKey, rightCollection, rightKey, joiner)

  def hashJoin(leftKey: Fields, rightCollection: Tube, rightKey: Fields, joiner: Joiner = new InnerJoin) = this << new HashJoin(pipe, leftKey, rightCollection, rightKey, joiner)

  def unique(fields: Fields) = this << new Unique(pipe, fields)
}

trait RowOperator {
  this: Tube =>

  def each(input: Fields = Fields.ALL, function: cascading.operation.Function[Any], output: Fields = Fields.ALL) = this << new Each(pipe, input, function, output)

  def filter(fields: Fields = Fields.ALL, filter: Filter[Any]) = this << new Each(pipe, fields, filter)
}

trait GroupOperator {
  this: Tube =>

  def every(input: Fields = Fields.ALL, buffer: Buffer[Any], output: Fields = Fields.RESULTS) = this << new Every(pipe, input, buffer, output)

  def top(group: Fields, sort: Fields, reverse: Boolean = false, limit: Int = 1) = {
    groupBy(group, sort, reverse)
    this << new Every(pipe, Fields.VALUES, new First(limit))
  }
}

trait FieldsTransform {
  this: Tube =>

  def discard(field: Fields) = this << new Discard(pipe, field)

  def rename(from: Fields, to: Fields) = this << new Rename(pipe, from, to)

  def retain(fields: Fields) = this << new Retain(pipe, fields)

  def insert(field: Fields, value: String*) = this << new Each(pipe, new Insert(field, value: _*), Fields.ALL)
}

trait MathOperation {
  this: Tube =>
  def divide(leftOp: String, rightOp: String, outField: Option[String] = None) = {
    val tmp = f(s"${leftOp}_")
    val divExpression = new ExpressionFunction(tmp, s"(double) $leftOp / (double) $rightOp", classOf[Double]).asInstanceOf[cascading.operation.Function[Any]]
    this << each((leftOp, rightOp), divExpression)
    if (outField.isDefined) {
      this << rename(tmp, outField.get)
    } else {
      this << discard(leftOp) << rename(tmp, leftOp)
    }
  }

  def multiply(leftOp: String, rightOp: String, outField: String) = {
    val multiplyExpression = new ExpressionFunction(outField, s"(double) $leftOp * (double) $rightOp", classOf[Double]).asInstanceOf[cascading.operation.Function[Any]]
    this << each((leftOp, rightOp), multiplyExpression)
  }
}
