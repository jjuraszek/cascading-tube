package jj.tube

import cascading.tuple.{Fields, TupleEntry}
import cascading.operation._
import cascading.flow.FlowProcess
import scala.collection.convert.WrapAsScala.asScalaIterator

object CustomOps {
  def asFilter(isRemovable: (TupleEntry => Boolean)): Filter[Any] = {
    new BaseOperation[Any] with Filter[Any] {
      override def isRemove(flowProcess: FlowProcess[_], call: FilterCall[Any]): Boolean = {
        val arg: TupleEntry = call.getArguments
        isRemovable(arg)
      }
    }
  }

  def asFunction(transform: (TupleEntry => TupleEntry)) = {
    new BaseOperation[Any] with Function[Any] {
      override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
        functionCall.getOutputCollector.add(transform(functionCall.getArguments))
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }
  }

  def asBuffer(transform: (TupleEntry, Iterator[TupleEntry]) => List[TupleEntry]) = {
    new BaseOperation[Any] with Buffer[Any] {
      override def operate(flowProcess: FlowProcess[_], bufferCall: BufferCall[Any]) {
        transform(bufferCall.getGroup, bufferCall.getArgumentsIterator).foreach(bufferCall.getOutputCollector.add(_))
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }
  }
}
