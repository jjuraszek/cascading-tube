package jj.tube

import cascading.pipe.{CoGroup, Pipe}
import cascading.tuple.Fields
import tube._

package object tube {
  //fields conversions
  implicit def aggregateFields(fields: Seq[Fields]):Fields = fields.reduceLeft[Fields]( (f1,f2) => f1.append(f2))
  implicit def stringToField(fields: Seq[String]):Fields = new Fields(fields:_*)
}


object Tube {
  def apply(name: String) = new Tube(name)
  //pipe conversion aka boxing
  implicit def tubeToPipe(tube:Tube) = tube.pipe
}

class Tube(name: String, previous: Pipe = _, var pipe: Pipe = new Pipe(name, previous)) {
  def coGroup(leftKey:String*)(rightCollection:Tube, rightKey: String*) : Tube = {
    pipe = new CoGroup(pipe, leftKey, rightCollection.pipe, rightKey)
    this
  }
}
