package jj.tube

import cascading.pipe._
import cascading.tuple.Fields
import cascading.tuple.Fields._
import cascading.operation.{DebugLevel, Debug}
import scala.language.reflectiveCalls
import jj.tube.alterations.{RowOperator, MathOperator, GroupOperator, FieldsOperator}

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
}

class Tube(var pipe: Pipe) extends GroupOperator with RowOperator with FieldsOperator with MathOperator {
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
  def split(input: Fields = ALL)(filter: FILTER) =
    (Tube("positive_" + pipe.getName, this.pipe)
        .filter(filter).withInput(input).go,
     Tube("negative_" + pipe.getName, this.pipe)
        .filterNot(filter).withInput(input).go)

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








