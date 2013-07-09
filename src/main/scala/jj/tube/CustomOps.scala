package jj.tube

import cascading.tuple.Fields
import cascading.operation._
import cascading.flow.FlowProcess
import scala.collection.convert.WrapAsScala.asScalaIterator
import cascading.pipe.assembly.{AverageBy, FirstBy, CountBy, SumBy}

object CustomOps extends Aggregators {
  def asFilter(isRemovable: (Map[String, String] => Boolean)): Filter[Any] =
    new BaseOperation[Any] with Filter[Any] {
      override def isRemove(flowProcess: FlowProcess[_], call: FilterCall[Any]): Boolean = {
        val arg = toMap(call.getArguments)
        isRemovable(arg)
      }
    }

  def asFunction(transform: (Map[String, String] => Map[String, Any])) =
    new BaseOperation[Any] with Function[Any] {
      override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]) {
        val mapOfOutcome = transform(toMap(functionCall.getArguments))
        functionCall.getOutputCollector.add(toTupleEntry(mapOfOutcome))
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }

  def asBuffer(transform: (Map[String, String], Iterator[Map[String, String]]) => List[Map[String, Any]]) =
    new BaseOperation[Any] with Buffer[Any] {
      override def operate(flowProcess: FlowProcess[_], bufferCall: BufferCall[Any]) {
        val lazyIterator = bufferCall.getArgumentsIterator.map(toMap)
        val group = toMap(bufferCall.getGroup)
        transform(group, lazyIterator)
          .map(toTupleEntry)
          .foreach(bufferCall.getOutputCollector.add)
      }

      def setOutputScheme(field: Fields) = {
        fieldDeclaration = field
        this
      }
    }
}

trait Aggregators {
  /**
   * Sum values from {@code field} resulting in type {@code resultType}. Must be part of Tube.aggregateBy call.
   */
  def sum(field: String, resultType: Class[_]): SumBy = new SumBy(field, field, resultType)

  /**
   * Sum values from {@code field} resulting in type {@code Double}. Must be part of Tube.aggregateBy call.
   */
  def sum(field: String): SumBy = sum(field, classOf[Double])

  /**
   * Sum values from {@code field} resulting in type {@code Integer}. Must be part of Tube.aggregateBy call.
   */
  def sumInt(field: String) = sum(field, classOf[Integer])

  /**
   * Count rows resulting in field {@code outField}. Must be part of Tube.aggregateBy call.
   */
  def count(outField: String) = new CountBy(outField)

  /**
   * Rewrite {@code field} from first row. Must be part of Tube.aggregateBy call.
   */
  def first(field: String) = new FirstBy(field)

  /**
   * Compute average value of {@code field}. Must be part of Tube.aggregateBy call.
   */
  def avg(field: String) = new AverageBy(field, field)
}
