package jj.tube.testing

import cascading.flow.FlowDef
import cascading.operation.DebugLevel
import cascading.flow.local.LocalFlowConnector
import jj.tube.io.tap.MemTap
import cascading.pipe.Pipe
import jj.tube.testing.BaseFlowTest.FlowRunner

object BaseFlowTest {

  private def tupleToArray(product: Product) = product.productIterator.collect[String]{
    case s:String => s
  }.toArray
  
  case class Source(schema: Array[String], data: List[Array[String]])

  class FlowRunner {
    val flowDef = FlowDef.flowDef()
    val asserts = scala.collection.mutable.Map.empty[MemTap,Set[String] => _]

    def withSource(start: Pipe, input:Source) = {
      flowDef.addSource(start, MemTap.input(input.data, input.schema))
      this
    }

    def withOutput(end:Pipe, assert: (Set[String] => Any) = null) = {
      val out = MemTap.output()
      flowDef.addTailSink(end, out)
      if(assert != null)asserts.put(out, assert)
      this
    }

    def compute = {
      flowDef.setDebugLevel(DebugLevel.VERBOSE)
      new LocalFlowConnector().connect(flowDef).complete()
      asserts.foreach{
        case (out, assert) => assert(out.content)
      }
      this
    }
  }
}

//base testing class including boilerplate
trait BaseFlowTest {
  implicit def singleFieldScheme(scheme: String) = Array(scheme)
  implicit def singleFieldData(data: List[String]) = data.map(Array(_)).toList

  implicit def tupleScheme(scheme: Product) = BaseFlowTest.tupleToArray(scheme)
  implicit def tupleData(data: List[Product]) = data.map(BaseFlowTest.tupleToArray).toList

  @deprecated("to be remove in ver.4","3.0.0")
  def runFlow(flowDef: FlowDef) = {
    flowDef.setDebugLevel(DebugLevel.VERBOSE)
    new LocalFlowConnector().connect(flowDef).complete()
  }
  def runFlow = new FlowRunner()

  @deprecated("to be remove in ver.4","3.0.0")
  def inTap(scheme: Array[String], data: List[Array[String]]) = MemTap.input(data, scheme)
  @deprecated("to be remove in ver.4","3.0.0")
  def outTap = MemTap.output()
}
