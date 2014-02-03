package jj.tube.builders

import cascading.tuple.Fields



trait WithOperationResult[T]{this: T =>
  var declaringScheme:Fields = null
  var resultScheme:Fields = null

  /**
   * declare transformation output
   */
  def declaring(declaringScheme: Fields) = {
    this.declaringScheme = declaringScheme
    this
  }

  /**
   * declare final output schema for grouping operation
   */
  def withResult(resultScheme: Fields) = {
    this.resultScheme = resultScheme
    this
  }
}
