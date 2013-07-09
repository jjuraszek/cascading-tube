package jj.tube

import cascading.tap.{SinkMode, Tap}
import cascading.tap.hadoop.Hfs
import cascading.scheme.hadoop.TextDelimited
import cascading.tuple.Fields

class Io(val tap: Tap[Any, Any, Any])

object Io {
  def flexCsvIn(path: String, scheme: List[String], delimiter: String = ",", escape: String = "\"") = {
    new Io(new Hfs(new TextDelimited(new Fields(scheme: _*), null, false, false, delimiter, false, escape, null, true), path).asInstanceOf[Tap[Any, Any, Any]])
  }

  def replacingCsvOut(path: String, delimiter: String = ",", escape: String = "\"") = {
    new Io(new Hfs(new TextDelimited(false, delimiter, escape), path, SinkMode.REPLACE).asInstanceOf[Tap[Any, Any, Any]])
  }

  implicit def toTap(io: Io): Tap[Any, Any, Any] = io.tap

  implicit def toIo(tap: Tap[Any, Any, Any]) = new Io(tap)
}

