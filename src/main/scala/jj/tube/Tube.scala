package jj.tube

import cascading.pipe.{CoGroup, Pipe}
import cascading.tuple.Fields

package object tube {

}


object Tube {
  def apply(name: String) = new Tube(name)
}

class Tube(name: String, previous: Pipe = _, var pipe: Pipe = new Pipe(name, previous)) {
  def coGroup(leftKey: Fields, rightCollection:Tube, rightKey: Fields) : Tube = {
    pipe = new CoGroup(pipe, leftKey, rightCollection.pipe, rightKey)
    this
  }
}
