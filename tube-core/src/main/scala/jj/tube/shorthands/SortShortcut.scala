package jj.tube.shorthands

import cascading.tuple.Fields
import java.util.Comparator

trait SortShortcut{
  /**
   * Define the sort order for declared fields and apply correct comparator for them
   * @param reverse
   */
  //TODO allow custom comparators by order builder
  sealed case class SortOrder(sortedFields: Fields, reverse: Boolean = false) {
    (0 until sortedFields.size).foreach {
      sortedFields.setComparator(_, new Comparator[Comparable[Any]] with Serializable {
        def compare(left: Comparable[Any], right: Comparable[Any]): Int = {
          if (reverse) right compareTo left
          else left compareTo right
        }
      })
    }

    val isAscending = !reverse
  }

  /** create desc order for fields*/
  def DESC(sortedFields: Fields) = SortOrder(sortedFields, reverse = true)
  /** create asc order for fields*/
  def ASC(sortedFields: Fields) = SortOrder(sortedFields, reverse = false)
}
