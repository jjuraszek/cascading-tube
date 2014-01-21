package jj.tube.builders

import jj.tube._
import scala.language.{reflectiveCalls,existentials}
import cascading.pipe.Each
import jj.tube.RichTupleEntry
import cascading.operation.{FilterCall, BaseOperation, Filter}
import cascading.flow.FlowProcess
import cascading.tuple.Fields

class FilterBuilder(val baseStream: Tube) extends  OperationBuilder
  with WithCustomOperation[FilterBuilder,RichTupleEntry => Boolean]{
  withInput(Fields.ALL)

  def go =
    baseStream << new Each(baseStream, input, asFilter(operation))

  def asFilter(isRemovable: (RichTupleEntry => Boolean)): Filter[Any] =
    new BaseOperation[Any] with Filter[Any] {
      override def isRemove(flowProcess: FlowProcess[_], call: FilterCall[Any]) = isRemovable(call.getArguments)
    }
}
