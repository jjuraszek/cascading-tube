package jj.tube.util

import java.util.Comparator

class StandardComparator(val reverse: Boolean) extends Comparator[Comparable[Any]] with Serializable {
  def compare(left: Comparable[Any], right: Comparable[Any]): Int = {
    val res = if (left == null && right == null) 0
    else if (left != null && right == null) 1
    else if (left == null && right != null) 1
    else left compareTo right

    res * (if(reverse)-1 else 1)
  }
}
