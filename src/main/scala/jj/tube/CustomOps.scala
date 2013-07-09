package jj.tube

import cascading.tuple.{Tuple, Fields, TupleEntry}
import cascading.operation._
import cascading.flow.FlowProcess
import scala.collection.convert.WrapAsScala.asScalaIterator

object CustomOps {
  def asFilter(isRemovable: (Map[String, String] => Boolean)): Filter[Any] = {
    new BaseOperation[Any] with Filter[Any] {
      override def isRemove(flowProcess: FlowProcess[_], call: FilterCall[Any]): Boolean = {
        val arg = toMap(call.getArguments)
        isRemovable(arg)
      }
    }
  }

  def asFunction(transform: (Map[String, String] => Map[String, Any])) = {
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
  }

  def asBuffer(transform: (Map[String, String], Iterator[Map[String, String]]) => List[Map[String, Any]]) = {
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

  def toTupleEntry(schemeWithValues: Map[String, Any]) =
    schemeWithValues.foldLeft(new TupleEntry(toField(schemeWithValues.keys.toSeq), Tuple.size(schemeWithValues.size))) {
      (te, entry) =>
        entry._2 match {
          case x: Boolean => te.setBoolean(entry._1, x)
          case x: Int => te.setInteger(entry._1, x)
          case x: Double => te.setDouble(entry._1, x)
          case x => te.setString(entry._1, if (x != null) x.toString() else "")
        }
        te
    }

  def toMap(tupleEntry: TupleEntry) = {
    val fieldWithVal = for {
      i <- 0 until tupleEntry.getFields.size
    } yield {
      tupleEntry.getFields.get(i).toString -> Option(tupleEntry.getObject(i)).getOrElse("").toString
    }
    fieldWithVal.toMap
  }
}
