package jj.tube.shorthands

import cascading.tuple.Fields
import java.util.Comparator
import jj.tube.util.StandardComparator

trait SortShortcut{
  /**
   * Define the sort order for declared fields and apply correct comparator for them
   * @param reverse negate default comparator
   */
  //TODO allow custom comparators by order builder
  abstract sealed class SortOrder(val reverse: Boolean) {
    val sortedFields: Fields
    if(!sortedFields.isUnknown)(0 until sortedFields.size).foreach {
      sortedFields.setComparator(_, new StandardComparator(reverse))
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
