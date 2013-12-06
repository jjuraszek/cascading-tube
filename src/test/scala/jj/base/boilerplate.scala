package jj.base

import cascading.flow.FlowDef
import cascading.operation.DebugLevel
import cascading.flow.local.LocalFlowConnector
import jj.tube.testing.tap.MemTap

trait CascadingFlowTest {
  def runFlow(flowDef: FlowDef) = {
    flowDef.setDebugLevel(DebugLevel.VERBOSE)
    new LocalFlowConnector().connect(flowDef).complete()
  }

  def in(scheme: Array[String], data: Set[Array[String]]) = MemTap.input(scheme, data)

  def out = MemTap.output()
}
