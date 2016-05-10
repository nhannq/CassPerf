/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author nhannq
 */
public class PutDataNonStop {
  private static Logger logger = LoggerFactory.getLogger(PutDataNonStop.class);

  public void generateDataforCassandraDatastax(int uID, int noOfReplicas, int startTimeStamp,
      int timeStampInterval, int nbstreams, String consistencyLevel, int noOfSamples,
      int maxBatchStmts) {
    int tsID = startTimeStamp;

    String timeStampOutput = "";
    // String test = "";

    long startTime = System.currentTimeMillis();
    try {
      uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF iRFCF =
          new uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF(Integer.MAX_VALUE,
              timeStampInterval, consistencyLevel, maxBatchStmts);
      startTime = System.currentTimeMillis();

      while (tsID < noOfSamples) {
        iRFCF.executeMultiColumnsNonStop(uID, tsID);
        tsID += maxBatchStmts;
      }

      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " in "
          + (System.currentTimeMillis() - startTime) + " microSec");
    } catch (Exception e) {// Catch exception if any
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " in "
          + (System.currentTimeMillis() - startTime) + " microSec");
      e.printStackTrace();
    }
  }
}
