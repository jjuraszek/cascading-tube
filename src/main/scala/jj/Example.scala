package jj

import jj.tube._
import cascading.tuple.Fields

object Example extends App {
  val user = Tube("user")
  val car = Tube("car")

  val fi:Fields = ("a","b")
  println(fi.getClass)
  //user.coGroup()
}
