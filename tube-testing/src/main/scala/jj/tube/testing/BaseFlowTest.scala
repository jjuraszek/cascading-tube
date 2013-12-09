package jj.tube.testing

import cascading.flow.FlowDef
import cascading.operation.DebugLevel
import cascading.flow.local.LocalFlowConnector
import jj.tube.io.tap.MemTap

//base testing class including boilerplate
trait BaseFlowTest {
  def runFlow(flowDef: FlowDef) = {
    flowDef.setDebugLevel(DebugLevel.VERBOSE)
    new LocalFlowConnector().connect(flowDef).complete()
  }

  def in(scheme: Array[String], data: Set[Array[String]]) = MemTap.input(data, scheme:_*)

  def out = MemTap.output()
}
