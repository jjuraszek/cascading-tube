package jj.tube.shorthands

import cascading.tuple.Fields
import java.util.Comparator

trait SortShortcut{
  /**
   * Define the sort order for declared fields and apply correct comparator for them
   * @param reverse negate output stream
   */
  //TODO allow custom comparators by order builder
  abstract sealed class SortOrder(val reverse: Boolean) {
    val sortedFields: Fields
    if(!sortedFields.isUnknown)(0 until sortedFields.size).foreach {
      sortedFields.setComparator(_, new Comparator[Comparable[Any]] with Serializable {
        def compare(left: Comparable[Any], right: Comparable[Any]): Int = {
          left compareTo right
        }
      })
    }

    val isAscending = !reverse
  }

  /** create desc order for fields*/
  case class DESC(sortedFields: Fields) extends SortOrder(true)
  /** create asc order for fields*/
  case class ASC(sortedFields: Fields) extends SortOrder(false)
  /** no sort indicator**/
  case class NO_SORT(sortedFields: Fields= Fields.UNKNOWN) extends SortOrder(false)
}
