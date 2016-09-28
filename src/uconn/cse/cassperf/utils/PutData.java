/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.naming.ldap.StartTlsRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author nhannq
 */
public class PutData {
	private static Logger logger = LoggerFactory.getLogger(PutData.class);

	public void generateDataforCassandraDatastax(int uID, int noOfReplicas, int minute, int rate, int startTimeStamp,
			int timeStampInterval, int nbstreams, String consistencyLevel, int maxBatchStmts, int testCassBatchStmtPerf,
			int valueType, int valueLength) {
		int tsID = startTimeStamp;
		long executedTime = 0;
		String timeStampOutput = "";
		int noOfSamples = minute * rate * 60 * nbstreams;
		int newRate = nbstreams * rate;
		try {
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
			uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF iRFCF = new uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF(
					newRate, timeStampInterval, consistencyLevel, -1, valueType, valueLength);
			Double Value = 0.0;

			// System.out.println(rate);
			// System.out.println(noOfSamples);
			// System.out.println(minute);
			long firstStartTime = System.currentTimeMillis();
			long remainingTime = 0;
			if (valueType == 0) { //double
//				if (rate > 1000) {
					Date date = new Date();
					String startTime = dateFormat.format(date); // get the time
																// when
					// sensor starts to put
					// data to the backend
					System.out.println("startTime-" + startTime);
					long startRunTime = System.currentTimeMillis();
					if (testCassBatchStmtPerf == 0) {
						while (tsID < noOfSamples - newRate + 1) {
							long timeStart = System.currentTimeMillis();
							iRFCF.executeMultiColumnsDoubleValue(uID, tsID);
							tsID += newRate;

							if ((System.currentTimeMillis() - timeStart) < 1000) {
								remainingTime += 1000 - (System.currentTimeMillis() - timeStart);
								Thread.sleep(1000 - (System.currentTimeMillis() - timeStart));
							}

							if ((System.currentTimeMillis() - firstStartTime) > minute * 60 * 1000) {
								System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
										+ executedTime + " milliSec");
								System.out.println("rate:" + rate);
								System.out.println("drop:" + (noOfSamples - tsID));
								System.out.println("ratio:" + tsID / noOfSamples);
								System.out.println("remainingTime:" + remainingTime);
								return;
							}
						}
					} else {
						System.out.println("Testing Cass Batch Statement Performance");
						while (tsID < noOfSamples - newRate + 1) {
							long timeStart = System.currentTimeMillis();
							iRFCF.executeMultiColumnsDoubleValue(uID, tsID);
							tsID += newRate;
						}
					}
					System.out.println("Execution time " + (System.currentTimeMillis() - startRunTime));
//				}
			} else if (valueType == 1) { //int
//				if (rate > 1000) {
					Date date = new Date();
					String startTime = dateFormat.format(date); // get the time
																// when
					// sensor starts to put
					// data to the backend
					System.out.println("startTime-" + startTime);
					long startRunTime = System.currentTimeMillis();
					if (testCassBatchStmtPerf == 0) {
						while (tsID < noOfSamples - newRate + 1) {
							long timeStart = System.currentTimeMillis();
							iRFCF.executeMultiColumnsIntValue(uID, tsID);
							tsID += newRate;

							if ((System.currentTimeMillis() - timeStart) < 1000) {
								remainingTime += 1000 - (System.currentTimeMillis() - timeStart);
								Thread.sleep(1000 - (System.currentTimeMillis() - timeStart));
							}

							if ((System.currentTimeMillis() - firstStartTime) > minute * 60 * 1000) {
								System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
										+ executedTime + " milliSec");
								System.out.println("rate:" + rate);
								System.out.println("drop:" + (noOfSamples - tsID));
								System.out.println("ratio:" + tsID / noOfSamples);
								System.out.println("remainingTime:" + remainingTime);
								return;
							}
						}
					} else {
						System.out.println("Testing Cass Batch Statement Performance");
						while (tsID < noOfSamples - newRate + 1) {
							long timeStart = System.currentTimeMillis();
							iRFCF.executeMultiColumnsIntValue(uID, tsID);
							tsID += newRate;
						}
					}
					System.out.println("Execution time " + (System.currentTimeMillis() - startRunTime));
//				}
			} else if (valueType == 2) { //text
//				if (rate > 1000) {
					Date date = new Date();
					String startTime = dateFormat.format(date); // get the time
																// when
					// sensor starts to put
					// data to the backend
					System.out.println("startTime-" + startTime);
					long startRunTime = System.currentTimeMillis();
					if (testCassBatchStmtPerf == 0) {
						while (tsID < noOfSamples - newRate + 1) {
							long timeStart = System.currentTimeMillis();
							iRFCF.executeMultiColumnsTextValue(uID, tsID);
							tsID += newRate;

							if ((System.currentTimeMillis() - timeStart) < 1000) {
								remainingTime += 1000 - (System.currentTimeMillis() - timeStart);
								Thread.sleep(1000 - (System.currentTimeMillis() - timeStart));
							}

							if ((System.currentTimeMillis() - firstStartTime) > minute * 60 * 1000) {
								System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
										+ executedTime + " milliSec");
								System.out.println("rate:" + rate);
								System.out.println("drop:" + (noOfSamples - tsID));
								System.out.println("ratio:" + tsID / noOfSamples);
								System.out.println("remainingTime:" + remainingTime);
								return;
							}
						}
					} else {
						System.out.println("Testing Cass Batch Statement Performance");
						while (tsID < noOfSamples - newRate + 1) {
							long timeStart = System.currentTimeMillis();
							iRFCF.executeMultiColumnsTextValue(uID, tsID);
							tsID += newRate;
						}
					}
					System.out.println("Execution time " + (System.currentTimeMillis() - startRunTime));
//				}
			}
			System.out.println(timeStampOutput);

			System.out.println(
					"Finish putting " + tsID + " " + noOfSamples + " " + rate + " in " + executedTime + " milliSec");
			System.out.println("rate:" + rate);
			System.out.println("drop:" + (noOfSamples - tsID));
			System.out.println("ratio:" + tsID / noOfSamples);
			System.out.println("remainingTime:" + remainingTime);
		} catch (Exception e) {// Catch exception if any
			System.out.println(timeStampOutput);
			System.out.println(
					"Finish putting " + tsID + " " + noOfSamples + " " + rate + " in " + executedTime + " milliSec");
			System.out.println("rate:" + rate);
			System.out.println("drop:" + (noOfSamples - tsID));
			System.out.println("ratio:" + tsID / noOfSamples);
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
