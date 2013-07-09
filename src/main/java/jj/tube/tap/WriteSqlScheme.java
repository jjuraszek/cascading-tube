package jj.tube.tap;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Schema for writing sql in batches.
 */
public class WriteSqlScheme extends Scheme<Object, Void, PreparedStatement, Void, Object> {
  private int batchSize = -1;
  private int currentBatch = 0;

  @Override
  public void sinkConfInit(FlowProcess<Object> flowProcess, Tap<Object, Void, PreparedStatement> tap, Object conf) {
    this.batchSize = ((SqlSinkTap) tap).batchSize;
  }

  @Override
  synchronized public void sink(FlowProcess<Object> flowProcess, SinkCall<Object, PreparedStatement> sinkCall) throws IOException {
    TupleEntry tuple = sinkCall.getOutgoingEntry();
    PreparedStatement ps = sinkCall.getOutput();
    try {
      for (int i = 0; i < tuple.size(); i++) {
        ps.setObject(i + 1, tuple.getObject(i));
      }
      ps.addBatch();
      currentBatch++;
      if (currentBatch > batchSize) {
        flushStatement(ps);
        currentBatch = 0;
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void sinkCleanup(FlowProcess<Object> flowProcess, SinkCall<Object, PreparedStatement> sinkCall) {
    try {
      if (currentBatch > 0) {
        flushStatement(sinkCall.getOutput());
      }
      sinkCall.getOutput().getConnection().commit();
      sinkCall.getOutput().getConnection().close();
      sinkCall.getOutput().close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  synchronized private void flushStatement(PreparedStatement ps) throws SQLException {
    ps.executeBatch();
    ps.clearBatch();
    ps.clearParameters();
  }

  @Override
  public boolean isSink() {
    return true;
  }

  @Override
  public void sourceConfInit(FlowProcess<Object> flowProcess, Tap<Object, Void, PreparedStatement> tap, Object conf) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean source(FlowProcess<Object> flowProcess, SourceCall<Void, Void> sourceCall) throws IOException {
    throw new UnsupportedOperationException();
  }
}
