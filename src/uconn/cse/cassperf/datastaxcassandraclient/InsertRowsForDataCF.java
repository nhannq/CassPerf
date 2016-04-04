/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package uconn.cse.cassperf.datastaxcassandraclient;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;

import me.prettyprint.hector.api.query.QueryResult;

/**
 * 
 * @author nhannguyen
 */
public class InsertRowsForDataCF extends InsertRows {
  int rate = -1;
  int maxBatchStmts = 65000;

  public InsertRowsForDataCF(int rate) {
    this.rate = rate;
    if (rate < maxBatchStmts)
      maxBatchStmts = rate;
    init();
  }

  public QueryResult<?> executeOneColumn(int rowName, int key) {

    // put nbKeys keys to Cassandra
    session.execute("INSERT INTO CassExp.Data (rowName, columnName, v) VALUES (" + rowName + ","
        + key + ", 1.0);");
    return null;
  }

  // put data through a list
  // public QueryResult<?> execute2(int RowName, int tsID, int rate) {
  public long executeMultiColumns(int rowName, int tsID, int rate) {
    // Key
    // String cqlCommand = "BEGIN BATCH ";
    // CF name, CF value
    int count = 0;
    while (count < rate) {
      BatchStatement bs = new BatchStatement();
      PreparedStatement ps =
          session.prepare("insert into CassExp.Data (rowName, columnName, v) VALUES (?,?,?)");
      for (int key = count; key < count + maxBatchStmts; key++) {
        bs.add(ps.bind(rowName, key, 1.0));
      }
      session.execute(bs);
      count += maxBatchStmts;
    }
    // cqlCommand += "APPLY BATCH;";
    // session.execute(cqlCommand);
    // mR.g

    // mutator.addInsertion(RowName, CFName, HFactory.createColumn(Key,
    // Value, LongSerializer.get(), LongSerializer.get()));
    // mutator.execute();
    return 1;
  }
}
