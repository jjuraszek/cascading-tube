package jj.tube.builders

import cascading.pipe.Each
import jj.tube._
import cascading.tuple.{TupleEntry, Fields}
import cascading.tuple.Fields._
import cascading.operation.{FunctionCall, BaseOperation, Function}
import cascading.flow.FlowProcess
import scala.language.{reflectiveCalls,existentials}

class FlatMapBuilder(val baseStream: Tube) extends OperationBuilder
  with WithCustomOperation[FlatMapBuilder,FUNCTION]
  with WithOperationResult[FlatMapBuilder]{

  declaring(UNKNOWN)
  withInput(ALL)

  def go =
    baseStream << new Each(baseStream, inputScheme, asFunction(operation).setOutputScheme(declaringScheme), resultScheme)

  def asFunction(transform: FUNCTION) =
    new BaseOperation[Any] with Function[Any] {
      override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
        transform(functionCall.getArguments)
          .foreach(WithCustomOperation.writeTupleEntryToOutput(_, functionCall.getOutputCollector))
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }
}

class MapBuilder(val baseStream: Tube) extends OperationBuilder
with WithCustomOperation[MapBuilder,SURJECTION]
with WithOperationResult[MapBuilder]{

  declaring(UNKNOWN)
  withInput(ALL)

  def go =
    baseStream << new Each(baseStream, inputScheme, asFunction(operation).setOutputScheme(declaringScheme), resultScheme)

  def asFunction(transform: SURJECTION) =
    new BaseOperation[Any] with Function[Any] {
      override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
        WithCustomOperation.writeTupleEntryToOutput(transform(functionCall.getArguments), functionCall.getOutputCollector)
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }
}
