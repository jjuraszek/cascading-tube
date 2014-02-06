package jj.tube.builders

import cascading.tuple.{TupleEntryCollector, TupleEntry, Fields}

object WithCustomOperation{
  def writeTupleEntryToOutput(tupleEntry:TupleEntry, collector: TupleEntryCollector) =
    if(tupleEntry.getTuple != null && tupleEntry.getTuple.size()>0){
      if(tupleEntry.getFields.isDefined)
        collector.add(tupleEntry)
      else collector.add(tupleEntry.getTuple)
    }
}

/**
 * allow apply custom operation like filter,buffer etc to Tube
 */
trait WithCustomOperation[T, OP] {this: T =>
  var inputScheme:Fields = null
  var operation: OP = _

  /**
   * define input of transformation
   */
  def withInput(input:Fields) = {this.inputScheme = input; this}

  /**
   * pass transformation function
   */
  def apply(operation: OP) = {this.operation = operation; this}
}
