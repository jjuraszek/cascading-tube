package jj.tube.builders

import jj.tube._
import cascading.tuple.Fields
import scala.language.{reflectiveCalls,existentials}
import cascading.pipe.Each
import jj.tube.RichTupleEntry
import cascading.operation.{FilterCall, BaseOperation, Filter}
import cascading.flow.FlowProcess

class FilterBuilder(val filter:RichTupleEntry => Boolean, val baseStream: Tube) extends  OperationBuilder{
  var input = Fields.ALL
  def withInput(input:Fields) = {this.input = input; this}

  def go =
    baseStream << new Each(baseStream, input, asFilter(filter))

  def asFilter(isRemovable: (RichTupleEntry => Boolean)): Filter[Any] =
    new BaseOperation[Any] with Filter[Any] {
      override def isRemove(flowProcess: FlowProcess[_], call: FilterCall[Any]) = isRemovable(call.getArguments)
    }
}
