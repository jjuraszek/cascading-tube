package jj.tube.util

import cascading.tuple.TupleEntry
import cascading.operation.BufferCall
import scala.collection.convert.WrapAsScala._

class TupleEntryIterable(bufferCall: BufferCall[Any], iteratorIndex: Int) extends Iterable[TupleEntry]{
  override def iterator =
    TupleEntriesIterator(
      bufferCall.getJoinerClosure.getIterator(iteratorIndex),
      bufferCall.getJoinerClosure.getValueFields()(iteratorIndex),
      bufferCall.getOutputCollector)
}
