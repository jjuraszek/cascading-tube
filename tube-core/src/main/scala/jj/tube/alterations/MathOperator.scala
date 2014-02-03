package jj.tube.alterations

import jj.tube._

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
    flatMap(leftOp, rightOp) {
      row =>
        Map(outField -> func(row(leftOp).toDouble, row(rightOp).toDouble))
    }.declaring(outField)

  /**
   * Math operation with single operand. {@code operand} must be convertable to Double.
   */
  def math(operand: String, outField: String)(func: Double => Double) =
    flatMap(operand) {
      row =>
        Map(outField -> func(row.double(operand)))
    }.declaring(outField).go
}
