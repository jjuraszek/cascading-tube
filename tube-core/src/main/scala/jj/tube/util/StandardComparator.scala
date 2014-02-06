package jj.tube.util

import java.util.Comparator

class StandardComparator(val reverse: Boolean) extends Comparator[Comparable[Any]] with Serializable {
  def compare(left: Comparable[Any], right: Comparable[Any]): Int = {
    if (reverse) right compareTo left
    else left compareTo right
  }
}
