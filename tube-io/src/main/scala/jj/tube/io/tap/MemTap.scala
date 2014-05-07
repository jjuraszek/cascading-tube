package jj.tube.io.tap

import cascading.util.{CloseableIterator, SingleValueCloseableIterator}
import cascading.tuple._
import cascading.tap.Tap
import cascading.flow.FlowProcess
import cascading.scheme.{Scheme, SinkCall, SourceCall, NullScheme}
import scala.collection.mutable.ListBuffer
import java.util.UUID
import jj.tube._

object MemTap {
  def input(data: List[Array[String]], fields: Array[String]) = new MemTap() {
    input = new SingleValueCloseableIterator[Iterator[Tuple]](data.map(new Tuple(_: _*)).iterator) {
      override def close() {}
    }

    override def getScheme: Scheme[Nothing, Iterator[Tuple], Nothing, Nothing, Nothing] =
      new NullScheme[Nothing, Iterator[Tuple], Nothing, Nothing, Nothing](fields, Fields.UNKNOWN) {
        override def source(flowProcess: FlowProcess[Nothing], sourceCall: SourceCall[Nothing, Iterator[Tuple]]): Boolean = {
          val newDate = sourceCall.getInput.hasNext
          if (newDate) sourceCall.getIncomingEntry.setTuple(sourceCall.getInput.next())
          newDate
        }
      }
  }

  def output() = new MemTap {
    override def getScheme: Scheme[Nothing, Iterator[Tuple], Nothing, Nothing, Nothing] =
      new NullScheme[Nothing, Iterator[Tuple], Nothing, Nothing, Nothing] {
        override def sink(flowProcess: FlowProcess[Nothing], sinkCall: SinkCall[Nothing, Nothing]) {
          val tuple = sinkCall.getOutgoingEntry
          val resultList:ListBuffer[String] = sinkCall.getOutput.asInstanceOf[ListBuffer[String]]
          resultList+=tuple.getTuple.toString(",")
        }
      }
  }
}

abstract class MemTap(val id:String =UUID.randomUUID().toString) extends Tap[Nothing, Iterator[Tuple], Nothing] {
  val result = ListBuffer.empty[String]
  var input: CloseableIterator[Iterator[Tuple]] = null

  override def openForRead(flowProcess: FlowProcess[Nothing], o: Iterator[Tuple]) =
    new TupleEntrySchemeIterator[Nothing, Iterator[Tuple]](flowProcess, getScheme, input.asInstanceOf[SingleValueCloseableIterator[Iterator[Tuple]]], getIdentifier)

  override def openForWrite(flowProcess: FlowProcess[Nothing], o: Nothing) =
    new TupleEntrySchemeCollector[Nothing, AnyRef](flowProcess, getScheme, result, getIdentifier)

  lazy val content = result.toSet

  override def getIdentifier = id
  override def setScheme(scheme: Scheme[Nothing, Iterator[Tuple], Nothing, _, _]) = super.setScheme(scheme)
  override def createResource(conf: Nothing) = throw new UnsupportedOperationException
  override def deleteResource(conf: Nothing) = throw new UnsupportedOperationException
  override def resourceExists(conf: Nothing) = throw new UnsupportedOperationException
  override def getModifiedTime(conf: Nothing) = throw new UnsupportedOperationException
}