package jj.tube.builders

import jj.tube._
import cascading.tuple.{TupleEntryCollector, TupleEntry}

/**
 * marking trait allowing implicit method transformation from builder to Tube
 */
trait OperationBuilder {
  def go:Tube
}
