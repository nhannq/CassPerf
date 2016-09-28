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
	int maxBatchStmts = 65000;
	int timeStampInterval = 1;
	ConsistencyLevel consistencyLevel;
	double valueDouble;
	int valueInt;
	String valueText;

	public InsertRowsForDataCF(int rate, int timeStampInterval, String consistencyLevelStr, int maxBatchStmts,
			int valueType, int valueLength) {

		this.rate = rate;
		this.timeStampInterval = timeStampInterval;
//		this.maxBatchStmts = maxBatchStmts;
		if (rate <= this.maxBatchStmts)
			this.maxBatchStmts = rate;
		else 
			this.maxBatchStmts = rate/2;
		System.out.println("maxBatchStmts: " + this.maxBatchStmts);
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

		if (valueType == 0) {			
			valueDouble = 1.0;
			if (valueLength == Integer.MAX_VALUE) 
				valueDouble = Double.MAX_VALUE;
		} else if (valueType == 1) {
			valueInt = valueLength;
		} else if (valueType == 2) {
			valueText = "";
			for (int i = 0; i < valueLength; i++) {
				valueText += "1";
			}
		}
		init();
	}

	public QueryResult<?> executeOneColumn(int rowName, int key) {

		// put nbKeys keys to Cassandra
		session.execute("INSERT INTO CassExp.Data (rowName, columnName, v) VALUES (" + rowName + "," + key + ", 1.0);");
		return null;
	}

	// put data through a list
	public void executeMultiColumnsDoubleValue(int rowName, int tsID) {
		// Key
		// String cqlCommand = "BEGIN BATCH ";
		// CF name, CF value
		int count = tsID;
		int batchSize = maxBatchStmts;
		int newTSID = tsID + rate;
		int nbLoops = rate / maxBatchStmts;
		// System.out.println("nbLoops " + nbLoops);
		BatchStatement bs;
		PreparedStatement ps;
		batchSize = rate % maxBatchStmts;

		// System.out.println("batch size " + batchSize);
		try {
			for (int i = 0; i < nbLoops; i++) {
				bs = new BatchStatement();
				bs.setConsistencyLevel(consistencyLevel);
				ps = session.prepare("insert into CassExp.Data (rowName, columnName, v) VALUES (?,?,?)");
				for (int key = count; key < count + maxBatchStmts; key += timeStampInterval)
					// System.out.println(key);
					bs.add(ps.bind(rowName, key, valueDouble));
				session.execute(bs);
				count += maxBatchStmts;
				// System.out.println("Count " + count);
			}

			if (batchSize > 0) {
				bs = new BatchStatement();
				bs.setConsistencyLevel(consistencyLevel);
				ps = session.prepare("insert into CassExp.Data (rowName, columnName, v) VALUES (?,?,?)");
				for (int key = count; key < count + batchSize; key += timeStampInterval)
					// System.out.println(key);
					bs.add(ps.bind(rowName, key, valueDouble));
				session.execute(bs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// session.executeAsync(bs);
		// //https://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html#executeAsync-com.datastax.driver.core.Statement-

		// System.out.println("count " + count);

		// cqlCommand += "APPLY BATCH;";
		// session.execute(cqlCommand);
		// mR.g

		// mutator.addInsertion(RowName, CFName, HFactory.createColumn(Key,
		// Value, LongSerializer.get(), LongSerializer.get()));
		// mutator.execute();
	}

	public void executeMultiColumnsIntValue(int rowName, int tsID) {
		// Key
		// String cqlCommand = "BEGIN BATCH ";
		// CF name, CF value
		int count = tsID;
		int batchSize = maxBatchStmts;
		int newTSID = tsID + rate;
		int nbLoops = rate / maxBatchStmts;
		// System.out.println("nbLoops " + nbLoops);
		BatchStatement bs;
		PreparedStatement ps;
		batchSize = rate % maxBatchStmts;

		// System.out.println("batch size " + batchSize);
		try {
			for (int i = 0; i < nbLoops; i++) {
				bs = new BatchStatement();
				bs.setConsistencyLevel(consistencyLevel);
				ps = session.prepare("insert into CassExp.Data (rowName, columnName, v) VALUES (?,?,?)");
				for (int key = count; key < count + maxBatchStmts; key += timeStampInterval)
					// System.out.println(key);
					bs.add(ps.bind(rowName, key, valueInt));
				session.execute(bs);
				count += maxBatchStmts;
				// System.out.println("Count " + count);
			}

			if (batchSize > 0) {
				bs = new BatchStatement();
				bs.setConsistencyLevel(consistencyLevel);
				ps = session.prepare("insert into CassExp.Data (rowName, columnName, v) VALUES (?,?,?)");
				for (int key = count; key < count + batchSize; key += timeStampInterval)
					// System.out.println(key);
					bs.add(ps.bind(rowName, key, valueInt));
				session.execute(bs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// session.executeAsync(bs);
		// //https://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html#executeAsync-com.datastax.driver.core.Statement-

		// System.out.println("count " + count);

		// cqlCommand += "APPLY BATCH;";
		// session.execute(cqlCommand);
		// mR.g

		// mutator.addInsertion(RowName, CFName, HFactory.createColumn(Key,
		// Value, LongSerializer.get(), LongSerializer.get()));
		// mutator.execute();
	}

	public void executeMultiColumnsTextValue(int rowName, int tsID) {
		// Key
		// String cqlCommand = "BEGIN BATCH ";
		// CF name, CF value
		int count = tsID;
		int batchSize = maxBatchStmts;
		int newTSID = tsID + rate;
		int nbLoops = rate / maxBatchStmts;
		// System.out.println("nbLoops " + nbLoops);
		BatchStatement bs;
		PreparedStatement ps;
		batchSize = rate % maxBatchStmts;

		// System.out.println("batch size " + batchSize);
		try {
			for (int i = 0; i < nbLoops; i++) {
				bs = new BatchStatement();
				bs.setConsistencyLevel(consistencyLevel);
				ps = session.prepare("insert into CassExp.Data (rowName, columnName, v) VALUES (?,?,?)");
				for (int key = count; key < count + maxBatchStmts; key += timeStampInterval)
					// System.out.println(key);
					bs.add(ps.bind(rowName, key, valueText));
				session.execute(bs);
				count += maxBatchStmts;
				// System.out.println("Count " + count);
			}

			if (batchSize > 0) {
				bs = new BatchStatement();
				bs.setConsistencyLevel(consistencyLevel);
				ps = session.prepare("insert into CassExp.Data (rowName, columnName, v) VALUES (?,?,?)");
				for (int key = count; key < count + batchSize; key += timeStampInterval)
					// System.out.println(key);
					bs.add(ps.bind(rowName, key, valueText));
				session.execute(bs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// session.executeAsync(bs);
		// //https://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html#executeAsync-com.datastax.driver.core.Statement-

		// System.out.println("count " + count);

		// cqlCommand += "APPLY BATCH;";
		// session.execute(cqlCommand);
		// mR.g

		// mutator.addInsertion(RowName, CFName, HFactory.createColumn(Key,
		// Value, LongSerializer.get(), LongSerializer.get()));
		// mutator.execute();
	}

	public void executeMultiColumnsNonStop(int rowName, int tsID) {
		BatchStatement bs = new BatchStatement();
		bs.setConsistencyLevel(consistencyLevel);

		PreparedStatement ps = session.prepare("insert into CassExp.Data (rowName, columnName, v) VALUES (?,?,?)");
		for (int key = tsID; key < tsID + maxBatchStmts; key += timeStampInterval) {
			// System.out.println(key);
			bs.add(ps.bind(rowName, key, 1.0));
		}
		session.execute(bs);
	}
}
