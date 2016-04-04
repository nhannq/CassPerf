package uconn.cse.cassperf.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

abstract public class AbstractCache<K, V> {

    protected final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    protected final Lock rLock = rwLock.readLock();
    protected final Lock wLock = rwLock.writeLock();
    protected int m_maxSize;
    protected BlockingQueue<K> m_queue;
    protected ConcurrentMap<K, V> m_cacheData;
    public static int cachehit;
    public static int cachemiss;
    private static Logger logger = Logger.getLogger(AbstractCache.class);

    public AbstractCache(int maxSize) {
        m_maxSize = maxSize;
        m_queue = new LinkedBlockingDeque<K>();
        m_cacheData = new ConcurrentHashMap<K, V>();

        cachehit = 0;
        cachemiss = 0;

        logger.info("Init " + AbstractCache.class + " cache size " + maxSize);
    }

    public void insertElement(K key, V value) {
        wLock.lock();
        try {
//            System.out.println("insert " + key);
            m_queue.add(key);
            m_cacheData.put(key, value);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            wLock.unlock();
        }
    }

    public boolean checkElement(K key) {
        rLock.lock();
        boolean res;
        try {
            res = m_cacheData.containsKey(key);
        } catch (Exception ex) {
            ex.printStackTrace();
            res = false;
        } finally {
            rLock.unlock();
        }

        if (res == true) {
            cachehit++;
        } else {
            cachemiss++;
        }

        return res;
    }

    public int getCachehit() {
        return cachehit;
    }

    public int getCacheMiss() {
        return cachemiss;
    }

    public V getElement(K key) {
        rLock.lock();
        V value = null;
        try {
            value = m_cacheData.get(key);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        } finally {
            rLock.unlock();
        }
        return value;
    }

    // Remove some items
    public void refresh() {
        try {
            int noofdeletedelments = 50000;
            System.out.println("size is " + getSize());
            int size = getSize() - m_maxSize;
            if (size > noofdeletedelments) {
//                System.out.println("Cleaning " + size + " elements");
                for (int i = 0; i < noofdeletedelments; i++) {
                    popItem();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void popItem() {
        wLock.lock();
        try {
            K key = m_queue.poll();
            m_cacheData.remove(key);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            wLock.unlock();
        }
    }

    public int getSize() {
        return m_queue.size();
    }

    public void resetCache() {
        wLock.lock();
        try {
            m_queue.clear();
            m_cacheData.clear();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            wLock.unlock();
        }
    }
}