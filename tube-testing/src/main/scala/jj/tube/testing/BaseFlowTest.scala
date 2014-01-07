package jj.tube.testing

import cascading.flow.FlowDef
import cascading.operation.DebugLevel
import cascading.flow.local.LocalFlowConnector
import jj.tube.io.tap.MemTap

object BaseFlowTest {
  private def tupleToArray(product: Product) = product.productIterator.collect[String]{
    case s:String => s
  }.toArray
}

//base testing class including boilerplate
trait BaseFlowTest {
  implicit def singleFieldScheme(scheme: String) = Array(scheme)
  implicit def singleFieldData(data: List[String]) = data.map(Array(_)).toList

  implicit def tupleScheme(scheme: Product) = BaseFlowTest.tupleToArray(scheme)
  implicit def tupleData(data: List[Product]) = data.map(BaseFlowTest.tupleToArray).toList

  def runFlow(flowDef: FlowDef) = {
    flowDef.setDebugLevel(DebugLevel.VERBOSE)
    new LocalFlowConnector().connect(flowDef).complete()
  }

  def inTap(scheme: Array[String], data: List[Array[String]]) = MemTap.input(data, scheme)

  def outTap = MemTap.output()
}
