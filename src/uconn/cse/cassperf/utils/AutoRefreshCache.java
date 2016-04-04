package uconn.cse.cassperf.utils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.log4j.Logger;

import uconn.cse.cassperf.hectorcassandraclient.GetDataFromDataCF;

public class AutoRefreshCache extends Thread {

    private int m_refreshFreq;
    @SuppressWarnings("rawtypes")
    private AbstractCache m_cache;
    private int[] tsIDs2;
    private int[] idx2;
    private int[] tsIDs;
    private int[] idx;
    int lsegment;
    int savedID1 = Integer.MAX_VALUE, savedID2 = Integer.MAX_VALUE, savedIDI = Integer.MAX_VALUE, savedIDI2 = Integer.MAX_VALUE;
    GetDataFromDataCF gS = new GetDataFromDataCF();
    int m_readCacheFreq = 50000;
    int noOfCompuations = 0;
    int[] mfidx;
    double[] data = new double[lsegment];
    protected final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    protected final Lock rLock = rwLock.readLock();
    protected final Lock wLock = rwLock.writeLock();
    private static Logger logger = Logger.getLogger(AutoRefreshCache.class);

    public AutoRefreshCache(String threadName, int freq,
            @SuppressWarnings("rawtypes") AbstractCache cache, int[] tsids2, int[] id2, int lsgment, int[] midx, int[] tsids, int[] id) {
        super(threadName);
        m_refreshFreq = freq;
        m_cache = cache;
        tsIDs = tsids;
        idx = id;
        tsIDs2 = tsids2;
        idx2 = id2;
        lsegment = lsgment;
        mfidx = midx;
        logger.info("Init " + AutoRefreshCache.class);
    }

    private double[] getData(int uID) {
        double[] TScurrentR = new double[lsegment];
//        double[] result = new double[lsegment];
//        int start = cSQ;
//        int end = cSQ + lsegment - 1;
        QueryResult<ColumnSlice<Integer, Double>> result0 = gS.execute2(uID, lsegment);
        ColumnSlice<Integer, Double> colslice0 = result0.get(); //get data from raw data table
        for (int i = 0; i < colslice0.getColumns().size(); i++) {
            TScurrentR[i] = colslice0.getColumns().get(i).getValue();
        }
//        System.out.println("result length " + result.length);
        return TScurrentR;
    }

    public void getEOGDataforCache() {
//        rLock.lock();
        int jj2 = mfidx[0];
        int jj = mfidx[1];
        int ii2 = mfidx[2];
        int ii = mfidx[3];
        if (ii2 != savedIDI && ii != savedIDI2) {
            savedIDI = ii2;
            savedIDI2 = ii;
            for (int i = ii; i > ii2; i--) {
                try {
                    if (!m_cache.checkElement(tsIDs[idx[i]])) {
                        data = getData(tsIDs[idx[i]]);
                        noOfCompuations++;
                        wLock.lock();
                        try {
//                            System.out.println("insert " + tsIDs[idx[i]]);
                            m_cache.insertElement(tsIDs[idx[i]], data);
                        } catch (Exception ex) {
                            System.out.println("Exception");
                            ex.printStackTrace();
                        } finally {
                            wLock.unlock();
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        if (jj2 != savedID1 && jj != savedID2) {
            savedID1 = jj2;
            savedID2 = jj;
//            System.out.println(jj2 + "  : " + jj);
            for (int i = jj; i > jj2; i--) {

                try {
                    if (!m_cache.checkElement(tsIDs2[idx2[i]])) {
                        data = getData(tsIDs2[idx2[i]]);
                        wLock.lock();
                        try {
//                            System.out.println("insert " + tsIDs2[idx2[i]]);
                            m_cache.insertElement(tsIDs2[idx2[i]], data);
                        } catch (Exception ex) {
                            System.out.println("Exception");
                            ex.printStackTrace();
                        } finally {
                            wLock.unlock();
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public void getIMGDataforCache() {
//        rLock.lock();
        int jj2 = mfidx[0];
        int jj = mfidx[1];
        if (jj2 != savedID1 && jj != savedID2) {
            savedID1 = jj2;
            savedID2 = jj;
//            System.out.println(jj2 + "  : " + jj);
            for (int i = jj; i > jj2; i--) {

                try {
                    if (!m_cache.checkElement(tsIDs2[idx2[i]])) {
                        data = getData(tsIDs2[idx2[i]]);
                        wLock.lock();
                        try {
//                            System.out.println("insert " + tsIDs2[idx2[i]]);
                            m_cache.insertElement(tsIDs2[idx2[i]], data);
                        } catch (Exception ex) {
                            System.out.println("Exception");
                            ex.printStackTrace();
                        } finally {
                            wLock.unlock();
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    @Override
    public void run() {
        int i = 0;
        while (true) {
            try {
                sleep(m_refreshFreq);
                i++;
//                System.out.println("Start refreshing cache...");
//				logger.info("Cache hit " + m_cache.cachehit);
//				logger.info("Cache miss " + m_cache.cachemiss);
//                
//				logger.info("Done refreshing cache...");
//                getEOGDataforCache();
//                getIMGDataforCache();
                if (i % 80000 == 0) {
                    System.out.println("Number of read request from thread is " + noOfCompuations);
                    m_cache.refresh();
                    i = 0;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}