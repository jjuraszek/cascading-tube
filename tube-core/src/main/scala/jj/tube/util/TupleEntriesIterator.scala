package jj.tube.util

import cascading.tuple.{TupleEntryCollector, TupleEntry}
import jj.tube.builders.WithCustomOperation

object TupleEntriesIterator {
  def apply(tupleEntryIterator: Iterator[TupleEntry], outputCollector: TupleEntryCollector) =
    new TupleEntriesIterator(tupleEntryIterator, outputCollector)
}

class TupleEntriesIterator(val rawIterator: Iterator[TupleEntry],
                               val outputCollector: TupleEntryCollector) extends Iterator[TupleEntry] {
  var current: TupleEntry = null

  override def hasNext = rawIterator.hasNext

  override def next() = {
    current = rawIterator.next()
    current
  }

  /**
   * Save current element to output stream. Remember that TupleEntries are mutable so you may save altered element if you intend to do so.
   */
  def saveCurrentTupleEntry() =
    if (current == null)
      throw new IllegalStateException("iterator has not yet been started")
    else {
      WithCustomOperation.writeTupleEntryToOutput(current, outputCollector)
      List.empty[TupleEntry]
    }
}
