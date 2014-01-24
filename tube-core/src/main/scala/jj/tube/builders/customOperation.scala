package jj.tube.builders

import cascading.tuple.Fields

/**
 * allow apply custom operation like filter,buffer etc to Tube
 */
trait WithCustomOperation[T, OP] {this: T =>
  var input:Fields = null
  var operation: OP = _

  /**
   * define input of transformation
   */
  def withInput(input:Fields) = {this.input = input; this}

  /**
   * pass transformation function
   */
  def apply(operation: OP) = {this.operation = operation; this}
}

trait WithOperationResult[T]{this: T =>
  var operationScheme:Fields = null
  var resultScheme:Fields = null

  /**
   * declare transformation output
   */
  def declaring(scheme: Fields) = {
    this.operationScheme = scheme
    this
  }

  /**
   * declare final output schema for grouping operation
   */
  def withResult(scheme: Fields) = {
    this.resultScheme = scheme
    this
  }
}
