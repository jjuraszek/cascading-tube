package jj.tube.tap;

import cascading.flow.FlowProcess;
import cascading.tap.SinkTap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqlSinkTap extends SinkTap {
  int batchSize = 1000;
  private String driver;
  private String jdbcURL;
  private String user;
  private String pass;
  private String insertQuery;
  private String id;

  private SqlSinkTap() {
    setScheme(new WriteSqlScheme());
  }

  public static SqlSinkTapBuilder builder() {
    return new SqlSinkTapBuilder();
  }

  @Override
  public String getIdentifier() {
    return id;
  }

  @Override
  public TupleEntryCollector openForWrite(FlowProcess flowProcess, Object outputCollector) throws IOException {
    try {
      loadDriver(driver);
      Connection conn = getConnection();
      conn.setAutoCommit(false);
      return new TupleEntrySchemeCollector(flowProcess, getScheme(), conn.prepareStatement(insertQuery), getIdentifier());
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  //TODO allow table creation
  @Override
  public boolean createResource(Object conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  //TODO allow for sql statement cleaning the table for current input
  @Override
  public boolean deleteResource(Object conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  //TODO check table existance
  @Override
  public boolean resourceExists(Object conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getModifiedTime(Object conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected Connection getConnection() throws SQLException {
    return DriverManager.getConnection(jdbcURL, user, pass);
  }

  private void loadDriver(String klass) {
    try {
      Class.forName(klass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Allow to build tap properly
   */
  private static class SqlSinkTapBuilder {
    /**
     * Modifiable instance of tap
     */
    public SqlSinkTap tap = new SqlSinkTap();

    public SqlSinkTapBuilder driver(String driver) {
      tap.driver = driver;
      return this;
    }

    public SqlSinkTapBuilder password(String pass) {
      tap.pass = pass;
      return this;
    }

    public SqlSinkTapBuilder user(String user) {
      tap.user = user;
      return this;
    }

    public SqlSinkTapBuilder url(String url) {
      tap.jdbcURL = url;
      return this;
    }

    public SqlSinkTapBuilder batchSize(int size) {
      tap.batchSize = size;
      return this;
    }

    public SqlSinkTapBuilder sql(String sql) {
      tap.insertQuery = sql;
      tap.id = sql.replaceAll("\\s+", " ");
      return this;
    }
  }
}
