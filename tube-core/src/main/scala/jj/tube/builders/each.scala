package jj.tube.builders

import cascading.pipe.Each
import jj.tube._
import cascading.tuple.Fields
import cascading.tuple.Fields._
import cascading.operation.{FunctionCall, BaseOperation, Function}
import cascading.flow.FlowProcess
import scala.language.{reflectiveCalls,existentials}

trait Mapper[T] extends OperationBuilder{ this: T =>
  var funcScheme = UNKNOWN
  var outScheme = ALL
  var function:RichTupleEntry => List[RichTupleEntry] = null

  def apply(function: RichTupleEntry => List[RichTupleEntry]) = {this.function = function; this}

  def asFunction(transform: (RichTupleEntry => List[RichTupleEntry])) =
    new BaseOperation[Any] with Function[Any] {
      override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
        transform(functionCall.getArguments).foreach{ richTupleEntry =>
          functionCall.getOutputCollector.add(richTupleEntry.tupleEntry)
        }
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }

  /**
   * declare transformation output
   */
  def declaring(scheme: Fields) = {
    this.funcScheme = scheme
    this
  }

  /**
   * declare final output schema for grouping operation
   */
  def withResult(scheme: Fields) = {
    this.outScheme = scheme
    this
  }
}

class EachBuilder(val input:Fields, val baseStream: Tube) extends Mapper[EachBuilder]{
  def go =
    baseStream << new Each(baseStream, input, asFunction(function).setOutputScheme(funcScheme), outScheme)
}
