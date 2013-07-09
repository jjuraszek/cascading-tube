package jj.tube

import cascading.pipe.{CoGroup, Pipe}
import cascading.pipe.joiner.{Joiner, InnerJoin}
import cascading.tuple.Fields

object Tube {
  def apply(name: String) = new Tube(new Pipe(name))

  //pipe conversion aka boxing
  implicit def toPipe(tube: Tube) = tube.pipe
  implicit def toTube(pipe: Pipe) = new Tube(pipe)
}

class Tube(var pipe: Pipe) {

  def coGroup(leftKey: Fields, rightCollection: Tube, rightKey: Fields, joiner:Joiner=new InnerJoin): Tube = {
    pipe = new CoGroup(pipe, leftKey, rightCollection, rightKey)
    this
  }

//  def groupBy()
}
