/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package uconn.cse.cassperf.datastaxcassandraclient;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import me.prettyprint.hector.api.query.QueryResult;

/**
 * 
 * @author nhannguyen
 */
public class InsertRowsForDataCF extends InsertRows {
  int rate = -1;
  int maxBatchStmts = 10000;
  int timeStampInterval = 1;
  ConsistencyLevel consistencyLevel;

  public InsertRowsForDataCF(int rate, int timeStampInterval, String consistencyLevelStr, int maxBatchStmts) {
    this.rate = rate;
    this.timeStampInterval = timeStampInterval;
    this.maxBatchStmts = maxBatchStmts;
    switch (consistencyLevelStr) {
      case "ALL":
        consistencyLevel = ConsistencyLevel.ALL;
        System.out.println("Consistency Level ALL");
        break;
      case "EACH_QUORUM":
        consistencyLevel = ConsistencyLevel.EACH_QUORUM;
        System.out.println("Consistency Level EACH QUORUM");
        break;
      case "ONE":
        consistencyLevel = ConsistencyLevel.ONE;
        System.out.println("Consistency Level ONE");
        break;
      case "TWO":
        consistencyLevel = ConsistencyLevel.TWO;
        System.out.println("Consistency Level TWO");
        break;
      case "LOCAL_ONE":
        consistencyLevel = ConsistencyLevel.LOCAL_ONE;
        System.out.println("Consistency Level LOCAL ONE");
        break;
      case "ANY":
        consistencyLevel = ConsistencyLevel.ANY;
        System.out.println("Consistency Level ANY");
        break;
      default:
        consistencyLevel = ConsistencyLevel.ONE;
        System.out.println("Consistency Level ONE");
        break;

    }
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
  public void executeMultiColumns(int rowName, int tsID) {
    // Key
    // String cqlCommand = "BEGIN BATCH ";
    // CF name, CF value
    int count = tsID;
    while (count < tsID + rate) {
      BatchStatement bs = new BatchStatement();
      bs.setConsistencyLevel(consistencyLevel);

      PreparedStatement ps =
          session.prepare("insert into CassExp.Data (rowName, columnName, v) VALUES (?,?,?)");
      for (int key = count; key < count + maxBatchStmts; key += timeStampInterval) {
        // System.out.println(key);
        bs.add(ps.bind(rowName, key, 1.0));
      }
      session.execute(bs);
      count += maxBatchStmts;
      // System.out.println("count " + count);
    }
    // cqlCommand += "APPLY BATCH;";
    // session.execute(cqlCommand);
    // mR.g

    // mutator.addInsertion(RowName, CFName, HFactory.createColumn(Key,
    // Value, LongSerializer.get(), LongSerializer.get()));
    // mutator.execute();
  }
}
