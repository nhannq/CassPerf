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

  public void generateDataforCassandraDatastax(int uID, int noOfReplicas, int rate,
      int startTimeStamp, int timeStampInterval, int nbstreams, String consistencyLevel, int noOfSamples, int maxBatchStmts) {
    int tsID = startTimeStamp;
    long executedTime = 0;
    String timeStampOutput = "";
    // String test = "";
    int newRate = nbstreams * rate;
    try {
      DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
      uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF iRFCF =
          new uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF(newRate,
              timeStampInterval, consistencyLevel, maxBatchStmts);
      Double Value = 0.0;


      // System.out.println(rate);
      // System.out.println(noOfSamples);
      // System.out.println(minute);
      long firstStartTime = System.currentTimeMillis();
      // double[] values = new double[rate];
      // for (int i = 0; i < rate; i++) {
      // values[i] = 1;
      // }

      if (rate > 1000) {
        // System.out.println("Here");
        Date date = new Date();
        String startTime = dateFormat.format(date); // get the time when
        // sensor starts to put
        // data to the backend
        System.out.println("startTime-" + startTime);
        while (tsID < noOfSamples - newRate + 1) {
          long timeStart = System.currentTimeMillis();
          // for (int i = 0; i < rate; i++) {
          // iRFCF.executeOneColumn(uID, tsID);
          // tsID += 1;
          // }
          iRFCF.executeMultiColumns(uID, tsID);
          tsID += newRate;
//          timeStampOutput += "ID : " + tsID + "-" + dateFormat.format(new Date()) + "\n";
        }
      }
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + executedTime + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
    } catch (Exception e) {// Catch exception if any
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + executedTime + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  } 
}
