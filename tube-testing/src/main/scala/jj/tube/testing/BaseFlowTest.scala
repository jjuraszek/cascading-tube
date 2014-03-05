package jj.tube.testing

import cascading.flow.FlowDef
import cascading.operation.DebugLevel
import cascading.flow.local.LocalFlowConnector
import jj.tube.io.tap.MemTap
import cascading.pipe.Pipe
import jj.tube.testing.BaseFlowTest.FlowRunner
import jj.tube.Tube

object BaseFlowTest {

  private def tupleToArray(product: Product):Array[String] = product.productIterator.collect[String]{
    case s:String => s
    case null => null
  }.toArray
  
  case class Source(schema: Array[String], data: List[Array[String]])

  class FlowRunner {
    val flowDef = FlowDef.flowDef()
    val asserts = scala.collection.mutable.Map.empty[MemTap,Set[String] => _]

    def withSource(start: Tube, input:Source) = {
      flowDef.addSource(start, MemTap.input(input.data, input.schema))
      this
    }

    def withOutput(end:Tube, assert: (Set[String] => Any) = null) = {
      val out = MemTap.output()
      flowDef.addTailSink(end, out)
      if(assert != null)asserts.put(out, assert)
      this
    }

    def compute = {
      runFromDef(flowDef)
      asserts.foreach{
        case (out, assert) => assert(out.content)
      }
      this
    }
  }

  def runFromDef(flowDef:FlowDef) = {
    flowDef.setDebugLevel(DebugLevel.VERBOSE)
    new LocalFlowConnector().connect(flowDef).complete()
  }
}

//base testing class including boilerplate
trait BaseFlowTest {
  implicit def singleFieldScheme(scheme: String) = Array(scheme)
  implicit def singleFieldData(data: List[String]) = data.map(Array(_)).toList

  implicit def toStringArray(tuple: Product) = BaseFlowTest.tupleToArray(tuple)

  def runFlow(flowDef: FlowDef) = BaseFlowTest.runFromDef(flowDef)
  def runFlow = new FlowRunner()
}
