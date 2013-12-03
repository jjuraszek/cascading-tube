package jj.base

import cascading.flow.FlowDef
import cascading.operation.DebugLevel
import cascading.flow.local.LocalFlowConnector
import jj.tube.tap.MemoryTap
import scala.collection.convert.WrapAsJava._

trait CascadingFlowTest {
  def runFlow(flowDef: FlowDef) = {
    flowDef.setDebugLevel(DebugLevel.VERBOSE)
    new LocalFlowConnector().connect(flowDef).complete
  }

  def in(scheme: Array[String], data: Set[Array[AnyRef]]) = MemoryTap.input(scheme, data)

  def out = MemoryTap.output()
}
