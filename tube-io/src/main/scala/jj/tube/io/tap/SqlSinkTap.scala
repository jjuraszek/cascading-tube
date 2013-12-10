package jj.tube.io.tap

import cascading.flow.FlowProcess
import cascading.tap.{Tap, SinkTap}
import cascading.tuple.TupleEntrySchemeCollector

import java.io.IOException
import java.sql.{PreparedStatement, DriverManager, SQLException}
import cascading.scheme.{SourceCall, SinkCall, Scheme}

object SqlSinkTap{
  def builder() = new SqlSinkTapBuilder()

  class SqlSinkTapBuilder {
    var insertQueryProp: String = _
    var userProp: String = _
    var passProp: String = _
    var jdbcURLProp: String = _
    var driverProp: String = _
    var batchSizeProp = 1000

    def tap = new SqlSinkTap(insertQueryProp, userProp, passProp, jdbcURLProp, driverProp, batchSizeProp)
    def driver(driver: String) = {this.driverProp = driver; this}
    def password(pass: String) = {this.passProp = pass; this}
    def user(user: String) = {this.userProp = user; this}
    def url(url: String) = {this.jdbcURLProp = url; this}
    def batchSize(size: Int) = {this.batchSizeProp = size; this}
    def sql(sql: String) = {this.insertQueryProp = sql; this}
  }
}


class SqlSinkTap(val insertQuery: String, val user: String, val pass: String, val jdbcURL: String, val driver: String, val batchSize: Int)
  extends SinkTap[Object, Object] {

  override def getIdentifier = insertQuery.replaceAll("\\s+", " ")

  override def openForWrite(flowProcess: FlowProcess[Object], o: Object) = try {
    Class.forName(driver)
    val conn = DriverManager.getConnection(jdbcURL, user, pass)
    conn.setAutoCommit(false)
    new TupleEntrySchemeCollector[Object, Object](flowProcess, getScheme, conn.prepareStatement(insertQuery).asInstanceOf[Object], getIdentifier())
  } catch {
    case e: SQLException => throw new IOException(e)
  }

  override def toString = s"SqlSinkTap{insertQuery=$insertQuery,batchSize=$batchSize }"

  override def createResource(conf: Object) = throw new UnsupportedOperationException
  override def deleteResource(conf: Object) = throw new UnsupportedOperationException
  override def resourceExists(conf: Object) = throw new UnsupportedOperationException
  override def getModifiedTime(conf: Object) = throw new UnsupportedOperationException

  setScheme(new Scheme[Object, Void, PreparedStatement, Object, Object] {
    var batchSize = -1
    var currentBatch = 0

    override def sinkConfInit(flowProcess: FlowProcess[Object], tap: Tap[Object, Void, PreparedStatement], conf: Object) {
      val sqlTap = tap.asInstanceOf[SqlSinkTap]
      batchSize = sqlTap.batchSize
    }

    override def sink(flowProcess: FlowProcess[Object], sinkCall: SinkCall[Object, PreparedStatement]) = this.synchronized {
      try {
        (0 until sinkCall.getOutgoingEntry.size()).foreach { i =>
          sinkCall.getOutput.setObject(i + 1, sinkCall.getOutgoingEntry.getObject(i))
        }
        sinkCall.getOutput.addBatch()
        currentBatch += 1
        if (currentBatch > batchSize) flushStatement(sinkCall.getOutput)
      } catch {
        case e: SQLException => throw new IOException(e)
      }
    }

    override def sinkCleanup(flowProcess: FlowProcess[Object], sinkCall: SinkCall[Object, PreparedStatement]) = this.synchronized {
      try {
        flushStatement(sinkCall.getOutput)
        sinkCall.getOutput.getConnection.commit()
        sinkCall.getOutput.getConnection.close()
        sinkCall.getOutput.close()
      } catch {
        case e: SQLException => throw new IOException(e)
      }
    }

    private def flushStatement(ps: PreparedStatement) {
      if (currentBatch > 0) {
        currentBatch = 0
        ps.executeBatch()
      }
      ps.clearBatch()
      ps.clearParameters()
    }

    override def isSink: Boolean = true

    override def sourceConfInit(flowProcess: FlowProcess[Object], tap: Tap[Object, Void, PreparedStatement], conf: Object) = throw new UnsupportedOperationException
    override def source(flowProcess: FlowProcess[Object], sourceCall: SourceCall[Object, Void]): Boolean = throw new UnsupportedOperationException
  }.asInstanceOf[Scheme[Object, Void, Object, Object, Object]])
}
