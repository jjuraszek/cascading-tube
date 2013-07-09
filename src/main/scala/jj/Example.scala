package jj

import jj.tube._
import cascading.tuple.{TupleEntry, Fields}
import cascading.tuple

object Example extends App {
  val user = Tube("user")
  val car = Tube("car")

  val fi: Fields = ("a", "b")
  println(fi.getClass)
  //user.coGroup()
  //val a:Function[Any] = asFunction({a:TupleEntry => new TupleEntry}, Fields.ALL)
  val c = "f" -> "d"
  println(c.getClass)
  println(c)
  Tube("df").each(Fields.ALL -> Fields.UNKNOWN, Fields.ALL) {
    row: TupleEntry => new TupleEntry
  }
}
