/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.model.ColumnList;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

/**
 * 
 * @author nhannguyen - uconn
 */
public class CheckData {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public void checkDatafromCassandraDatastax(int uID, int noOfReplicas, int minute,
      String lcheckfile, int rate, int delayTime, String logFileName) {
    int noOfSamples = minute * rate * 60;
    System.out.println("noOfSamples " + noOfSamples);
    int[] samplesPerMinute = new int[minute]; // stores the samples per
    // minute based on the time
    // stamp of the time when
    // the client send message
    // to server
    int[] samplesPerMinuteServerTime = new int[minute]; // stores the
    // samples per
    // minute based on
    // the time stamp of
    // the time when
    // Cassandra saves
    // message
    int nbDroppedMessages = -1;
    int nbPutMessages = 0;
    for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++) {
      samplesPerMinute[minuteIdx] = 0;
      samplesPerMinuteServerTime[minuteIdx] = 0;
    }
    int nbReadMessages = rate;
    int nbRuns = noOfSamples / nbReadMessages + 1;
    try {
      int tsID = 0;
      uconn.cse.cassperf.datastaxcassandraclient.GetDataFromDataCF iGFCF =
          new uconn.cse.cassperf.datastaxcassandraclient.GetDataFromDataCF();
      // DateFormat dateFormat = new
      // SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
      // Date date = new Date();
      String startTime = ""; // get the time when
      // sensor starts to put
      // data to the backend
//      BufferedReader br = new BufferedReader(new FileReader(logFileName));
//      try {
//        String line = br.readLine();
//        while (line != null) {
//          if (line.contains("startTime")) {
//            startTime = line.split("-")[1];
//          }
//          line = br.readLine();
//        }
//      } finally {
//        br.close();
//      }
      int count = 0;
      int size;
//      String[] parsedStartTime = startTime.split("\\s+");
//      System.out.println("Time client called server :" + parsedStartTime[0]);
//      System.out.println("Time client called server :" + parsedStartTime[1]);
      ResultSet result0 = iGFCF.execute("Data", uID, 0, nbReadMessages - 1, false, nbReadMessages);
      int LIMIT = nbReadMessages + 100;
      while (tsID < noOfSamples - rate + 1) {
        result0 =
            iGFCF.execute("CassExp.Data", uID, tsID, tsID + nbReadMessages - 1, false,
                LIMIT);
        tsID += nbReadMessages;
        size = result0.all().size();
        nbPutMessages += size;
//        System.out.println("tsID " + tsID + " size " + size);
//        System.out.println("nbPutMessages " + nbPutMessages);
        count++;
        if (count > nbRuns)
          break;
        // System.out.println("size " + sum + " : " + s);
      }
      // }
      nbDroppedMessages = noOfSamples - nbPutMessages;
      System.out.println("nbPutMessages " + nbPutMessages);
      System.out.println("Finish checking: " + tsID + " size " + nbDroppedMessages + " / "
          + noOfSamples + " : " + (nbDroppedMessages / noOfSamples*1.0));
      System.out.println("rate:" + rate);
      System.out.println("drop:" + nbDroppedMessages);
      System.out.println("ratio:" + nbDroppedMessages / noOfSamples);
      for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
        System.out.println("minute " + minuteIdx + " : " + samplesPerMinute[minuteIdx]);
      for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
        System.out.println("minuteServerTime " + minuteIdx + " : "
            + samplesPerMinuteServerTime[minuteIdx]);
    } catch (Exception e) {// Catch exception if any
      System.out.println("Finish checking: size " + nbDroppedMessages + " / " + noOfSamples + " : "
          + (nbDroppedMessages / noOfSamples));
      System.out.println("rate:" + rate);
      System.out.println("drop:" + nbDroppedMessages);
      System.out.println("ratio:" + nbDroppedMessages / noOfSamples);
      for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
        System.out.println("minute " + minuteIdx + " : " + samplesPerMinute[minuteIdx]);
      for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
        System.out.println("minuteServerTime " + minuteIdx + " : "
            + samplesPerMinuteServerTime[minuteIdx]);
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public void checkDatafromCassandraAstyanax(int uID, int noOfReplicas, int noOfSamples,
      String lcheckfile, int rate, int delayTime, String logFileName) {
    Double sum = 0.0;
    int minute = noOfSamples / (rate * 60) + delayTime;
    int[] samplesPerMinute = new int[minute]; // stores the samples per
    // minute based on the time
    // stamp of the time when
    // the client send message
    // to server
    int[] samplesPerMinuteServerTime = new int[minute]; // stores the
    // samples per
    // minute based on
    // the time stamp of
    // the time when
    // Cassandra saves
    // message
    for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++) {
      samplesPerMinute[minuteIdx] = 0;
      samplesPerMinuteServerTime[minuteIdx] = 0;
    }
    try {
      int tsID = 0;
      uconn.cse.cassperf.astyanaxcassandraclient.GetDataFromDataCF iGFCF =
          new uconn.cse.cassperf.astyanaxcassandraclient.GetDataFromDataCF();
      // DateFormat dateFormat = new
      // SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
      // Date date = new Date();
      String startTime = ""; // get the time when
      // sensor starts to put
      // data to the backend
      BufferedReader br = new BufferedReader(new FileReader(logFileName));
      try {
        String line = br.readLine();

        while (line != null) {
          if (line.contains("startTime")) {
            startTime = line.split("-")[1];
          }
          line = br.readLine();
        }
      } finally {
        br.close();
      }
      String[] parsedStartTime = startTime.split("\\s+");
      System.out.println("Time client called server :" + parsedStartTime[0]);
      System.out.println("Time client called server :" + parsedStartTime[1]);
      int startMinute = Integer.parseInt(parsedStartTime[1].split(":")[1]);
      int startSecond = Integer.parseInt(parsedStartTime[1].split(":")[2]);
      String putTime;
      int putMinute = 0;
      int putSecond = 0;
      String[] parsedPutTime;
      ColumnList<Integer> result0 = iGFCF.execute("Data", uID, 0, rate - 1, false, rate);
      String firstPutTime =
          new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(result0
              .getColumnByIndex(0).getTimestamp() / 1000));

      String[] parsedFirstPutTime = firstPutTime.split("\\s+");

      int firstPutMinute = Integer.parseInt(parsedFirstPutTime[1].split(":")[1]);
      int firstPutSecond = Integer.parseInt(parsedFirstPutTime[1].split(":")[2]);
      System.out.println("Time Casssandra saves data : " + parsedFirstPutTime[0]);
      System.out.println("Time Casssandra saves data : " + parsedFirstPutTime[1]);

      if ((startMinute >= 54 && startMinute <= 59)
          && (firstPutMinute >= 54 && firstPutMinute <= 59)) {
        // startMinute = startMinute - 60;
        while (tsID < noOfSamples - rate + 1) {
          result0 = iGFCF.execute("Data", uID, tsID, tsID + rate - 1, false, rate);

          if (result0.size() > 0) {
            sum += result0.size() * 1.0;
            // if (parsedStartTime[0].equals(parsedPutTime[0])) {
            // //if
            // the date of put time is similar to the date of start
            // time
            // System.out.println("Same day");
            // }
            // System.out.println(startTime);
            for (int columnIdx = 0; columnIdx < rate; columnIdx++) {
              putTime =
                  new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(
                      result0.getColumnByIndex(columnIdx).getTimestamp() / 1000));
              parsedPutTime = putTime.split("\\s+");
              // System.out.println(parsedPutTime[1]);
              // putHour =
              // Integer.parseInt(parsedPutTime[1].split(":")[0]);
              putMinute = Integer.parseInt(parsedPutTime[1].split(":")[1]);
              if (putMinute <= 5)
                putMinute += 60;
              putSecond = Integer.parseInt(parsedPutTime[1].split(":")[2]);
              for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++) {
                if ((putMinute == startMinute + minuteIdx && putSecond >= startSecond)
                    || (putMinute == startMinute + minuteIdx + 1 && putSecond < startSecond)) {
                  samplesPerMinute[minuteIdx]++;
                }
                if ((putMinute == firstPutMinute + minuteIdx && putSecond >= firstPutSecond)
                    || (putMinute == firstPutMinute + minuteIdx + 1 && putSecond < startSecond)) {
                  samplesPerMinuteServerTime[minuteIdx]++;
                }
              }
            }
          }
          tsID += rate;
        }
      }
      if ((startMinute >= 54 && startMinute <= 59) && (firstPutMinute < 54 || firstPutMinute > 59)) {
        // startMinute = startMinute - 60;
        while (tsID < noOfSamples - rate + 1) {
          result0 = iGFCF.execute("Data", uID, tsID, tsID + rate - 1, false, rate);
          if (result0.size() > 0) {
            sum += result0.size() * 1.0;
            // if (parsedStartTime[0].equals(parsedPutTime[0])) {
            // //if
            // the date of put time is similar to the date of start
            // time
            // System.out.println("Same day");
            // }
            // System.out.println(startTime);
            for (int columnIdx = 0; columnIdx < rate; columnIdx++) {
              putTime =
                  new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(
                      result0.getColumnByIndex(columnIdx).getTimestamp() / 1000));
              parsedPutTime = putTime.split("\\s+");
              // System.out.println(parsedPutTime[1]);
              // putHour =
              // Integer.parseInt(parsedPutTime[1].split(":")[0]);
              putMinute = Integer.parseInt(parsedPutTime[1].split(":")[1]);
              if (putMinute <= 5)
                putMinute += 60;
              putSecond = Integer.parseInt(parsedPutTime[1].split(":")[2]);
              for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++) {
                if ((putMinute == startMinute + minuteIdx && putSecond >= startSecond)
                    || (putMinute == startMinute + minuteIdx + 1 && putSecond < startSecond)) {
                  samplesPerMinute[minuteIdx]++;
                }
              }
            }
          }
          tsID += rate;
        }
      }
      if ((startMinute < 54 || startMinute > 59) && (firstPutMinute >= 54 && firstPutMinute <= 59)) {
        // startMinute = startMinute - 60;
        while (tsID < noOfSamples - rate + 1) {
          result0 = iGFCF.execute("Data", uID, tsID, tsID + rate - 1, false, rate);
          if (result0.size() > 0) {
            // sum += colslice0.getColumns().size() * 1.0;
            // if (parsedStartTime[0].equals(parsedPutTime[0])) {
            // //if
            // the date of put time is similar to the date of start
            // time
            // System.out.println("Same day");
            // }
            // System.out.println(startTime);
            for (int columnIdx = 0; columnIdx < rate; columnIdx++) {
              putTime =
                  new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(
                      result0.getColumnByIndex(columnIdx).getTimestamp() / 1000));
              parsedPutTime = putTime.split("\\s+");
              // System.out.println(parsedPutTime[1]);
              // putHour =
              // Integer.parseInt(parsedPutTime[1].split(":")[0]);
              putMinute = Integer.parseInt(parsedPutTime[1].split(":")[1]);
              if (putMinute <= 5)
                putMinute += 60;
              putSecond = Integer.parseInt(parsedPutTime[1].split(":")[2]);
              for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++) {
                if ((putMinute == firstPutMinute + minuteIdx && putSecond >= firstPutSecond)
                    || (putMinute == firstPutMinute + minuteIdx + 1 && putSecond < firstPutSecond)) {
                  samplesPerMinuteServerTime[minuteIdx]++;
                }
              }
            }
          }
          tsID += rate;
        }
      }
      if (startMinute < 54 || startMinute > 59) {
        tsID = 0;
        sum = 0.0;
        while (tsID < noOfSamples - rate + 1) {
          result0 = iGFCF.execute("Data", uID, tsID, tsID + rate - 1, false, rate);
          if (result0.size() > 0) {
            sum += result0.size() * 1.0;
            // if (parsedStartTime[0].equals(parsedPutTime[0])) {
            // //if
            // the date of put time is similar to the date of start
            // time
            // System.out.println("Same day");
            // }
            // System.out.println(startTime);
            for (int columnIdx = 0; columnIdx < rate; columnIdx++) {
              putTime =
                  new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(
                      result0.getColumnByIndex(columnIdx).getTimestamp() / 1000));
              parsedPutTime = putTime.split("\\s+");
              // System.out.println(parsedPutTime[1]);
              // putHour =
              // Integer.parseInt(parsedPutTime[1].split(":")[0]);
              putMinute = Integer.parseInt(parsedPutTime[1].split(":")[1]);
              putSecond = Integer.parseInt(parsedPutTime[1].split(":")[2]);
              for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++) {
                if ((putMinute == startMinute + minuteIdx && putSecond >= startSecond)
                    || (putMinute == startMinute + minuteIdx + 1 && putSecond < startSecond)) {
                  samplesPerMinute[minuteIdx]++;
                  break;
                }
              }
            }
          }
          tsID += rate;
        }
      }
      if (firstPutMinute < 54 || firstPutMinute > 59) {
        tsID = 0;
        while (tsID < noOfSamples - rate + 1) {
          result0 = iGFCF.execute("Data", uID, tsID, tsID + rate - 1, false, rate);
          if (result0.size() > 0) {
            // sum += dataColumns.size() * 1.0;
            // if (parsedStartTime[0].equals(parsedPutTime[0])) {
            // //if
            // the date of put time is similar to the date of start
            // time
            // System.out.println("Same day");
            // }
            // System.out.println(startTime);
            for (int columnIdx = 0; columnIdx < rate; columnIdx++) {
              putTime =
                  new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(
                      result0.getColumnByIndex(columnIdx).getTimestamp() / 1000));
              parsedPutTime = putTime.split("\\s+");
              // System.out.println(parsedPutTime[1]);
              // putHour =
              // Integer.parseInt(parsedPutTime[1].split(":")[0]);
              putMinute = Integer.parseInt(parsedPutTime[1].split(":")[1]);
              putSecond = Integer.parseInt(parsedPutTime[1].split(":")[2]);
              for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++) {
                if ((putMinute == firstPutMinute + minuteIdx && putSecond >= firstPutSecond)
                    || (putMinute == firstPutMinute + minuteIdx + 1 && putSecond < firstPutSecond)) {
                  samplesPerMinuteServerTime[minuteIdx]++;
                  break;
                }
              }
            }
          }
          tsID += rate;
        }
      }

      System.out.println("Finish checking: " + tsID + " size " + sum + " / " + noOfSamples + " : "
          + (sum / noOfSamples));
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - sum));
      System.out.println("ratio:" + sum / noOfSamples);
      for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
        System.out.println("minute " + minuteIdx + " : " + samplesPerMinute[minuteIdx]);
      for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
        System.out.println("minuteServerTime " + minuteIdx + " : "
            + samplesPerMinuteServerTime[minuteIdx]);
    } catch (Exception e) {// Catch exception if any
      System.out.println("Finish checking: sum " + sum + " / " + noOfSamples + " : "
          + (sum / noOfSamples));
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - sum));
      System.out.println("ratio:" + sum / noOfSamples);
      for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
        System.out.println("minute " + minuteIdx + " : " + samplesPerMinute[minuteIdx]);
      for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
        System.out.println("minuteServerTime " + minuteIdx + " : "
            + samplesPerMinuteServerTime[minuteIdx]);
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }

  }
}
