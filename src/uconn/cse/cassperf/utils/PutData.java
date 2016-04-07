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
public class PutData {
  private static Logger logger = LoggerFactory.getLogger(PutData.class);

  public void generateDataforCassandraDatastax(int uID, int noOfReplicas, int minute, int rate,
      int startTimeStamp, int timeStampInterval, int nbstreams, String consistencyLevel) {
    int tsID = startTimeStamp;
    long executedTime = 0;
    String timeStampOutput = "";
    // String test = "";
    int noOfSamples = minute * rate * 60 * nbstreams;
    int newRate = nbstreams * rate;
    try {
      DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
      uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF iRFCF =
          new uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF(newRate,
              timeStampInterval, consistencyLevel);
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
          if ((System.currentTimeMillis() - timeStart) < 1000)
            Thread.sleep(1000 - (System.currentTimeMillis() - timeStart));

          if ((System.currentTimeMillis() - firstStartTime) > minute * 60 * 1000) {
//            System.out.println("timeStampOutput " + timeStampOutput);
            System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
                + executedTime + " microSec");
            System.out.println("rate:" + rate);
            System.out.println("drop:" + (noOfSamples - tsID));
            System.out.println("ratio:" + tsID / noOfSamples);
            return;
          }
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

  public void generateDataforCassandraAstyanax(int uID, int noOfReplicas, int noOfSamples, int rate) {
    int tsID = 0;
    long executedTime = 0;
    String timeStampOutput = "";
    // String test = "";
    try {
      DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
      uconn.cse.cassperf.astyanaxcassandraclient.InsertRowsForDataCF iRFCF =
          new uconn.cse.cassperf.astyanaxcassandraclient.InsertRowsForDataCF();
      Double Value = 0.0;
      iRFCF.executeMultiColumns(uID, 0, 10);
      System.out.println("Sleeping 10 seconds");
      Thread.sleep(10000);

      int minute = noOfSamples / (rate * 60);
      long timeStart;
      // System.out.println(rate);
      // System.out.println(noOfSamples);
      // System.out.println(minute);

      // double[] values = new double[rate];
      // for (int i = 0; i < rate; i++) {
      // values[i] = 1;
      // }
      long timeout = (minute * 60 + 1) * 1000;// 1 second delay at when
      // the sensor puts data to
      // the server
      if (rate > 1000) {
        // System.out.println("Here");
        Date date = new Date();
        String startTime = dateFormat.format(date); // get the time when
        // sensor starts to put
        // data to the backend
        timeStampOutput += "startTime-" + startTime + "\n";
        long firstStartTime = System.currentTimeMillis();
        while (tsID < noOfSamples - rate + 1) {
          timeStart = System.currentTimeMillis();
          // for (int i = 0; i < rate; i++) {
          // iRFCF.executeOneColumn(uID, tsID);
          // tsID += 1;
          // }
          iRFCF.executeMultiColumns(uID, tsID, rate);
          tsID += rate;
          logger.info("ID : " + tsID + "-" + dateFormat.format(new Date()));
          timeStampOutput += "ID : " + tsID + "-" + dateFormat.format(new Date()) + "\n";
          if ((System.currentTimeMillis() - timeStart) < 1000)
            Thread.sleep(1000 - (System.currentTimeMillis() - timeStart));

          if ((System.currentTimeMillis() - firstStartTime) > timeout) {
            System.out.println(timeStampOutput);
            System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
                + executedTime + " microSec");
            System.out.println("rate:" + rate);
            System.out.println("drop:" + (noOfSamples - tsID));
            System.out.println("ratio:" + tsID / noOfSamples);
            return;
          }
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
      logger.debug("failed to write data to server", e);
      e.printStackTrace();
    }
  }

  public void generateDataforCassandraAstyanaxOneClient(int uID, int noOfReplicas, int noOfSamples,
      int rate) {
    System.out.println("generateDataforCassandraAstyanaxOneClient");
    int tsID = 0;
    long startTime = System.currentTimeMillis();
    String timeStampOutput = "";
    // String test = "";
    try {
      uconn.cse.cassperf.astyanaxcassandraclient.InsertRowsForDataCF iRFCF =
          new uconn.cse.cassperf.astyanaxcassandraclient.InsertRowsForDataCF();

      for (tsID = 0; tsID < noOfSamples; tsID++) {
        iRFCF.executeMultiColumns(uID, tsID, rate);
      }

      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + (System.currentTimeMillis() - startTime) + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
    } catch (Exception e) {// Catch exception if any
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + (System.currentTimeMillis() - startTime) + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
      System.err.println("Error: " + e.getMessage());
      logger.debug("failed to write data to server", e);
      e.printStackTrace();
    }
  }

}
