package jj.tube

import cascading.pipe.{CoGroup, Pipe}

object Tube {
  def apply(name: String) = new Tube(name)

  //pipe conversion aka boxing
  implicit def tubeToPipe(tube: Tube) = tube.pipe
}

class Tube(name: String, previous: Pipe = null) {
  var pipe: Pipe = new Pipe(name, previous)

  def coGroup(leftKey: String*)(rightCollection: Tube, rightKey: String*): Tube = {
    pipe = new CoGroup(pipe, leftKey, rightCollection, rightKey)
    this
  }
}
