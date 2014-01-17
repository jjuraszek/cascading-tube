package jj.tube.builders

import jj.tube._

/**
 * marking trait allowing implicit method transformation from builder to Tube
 */
trait OperationBuilder {
  def go:Tube
}
