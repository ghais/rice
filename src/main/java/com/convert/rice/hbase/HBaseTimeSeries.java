package com.convert.rice.hbase;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Instant;
import org.joda.time.Interval;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.RowInterval;
import com.convert.rice.TimeSeries;
import com.convert.rice.WritableDataPoints;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class HBaseTimeSeries implements TimeSeries {

    private static final int SCANNER_CACHING = new Integer(System.getProperty("scannerCaching", "1000"));

    /**
     * The default column family.
     */
    public static final byte[] CF = new byte[] { 'v' };

    private final Configuration conf;

    private final HTablePool pool;

    private final RowInterval rowInterval;

    private final boolean writeToWall;

    /**
     * The HTable associated with the metric type.
     * 
     * @param hTable
     */
    public HBaseTimeSeries(Configuration configuration, HTablePool pool) {
        this(configuration, pool, RowInterval.MINUTE);
    }

    public HBaseTimeSeries(Configuration conf, HTablePool pool, RowInterval rowInterval) {
        this(conf, pool, false, rowInterval);
    }

    public HBaseTimeSeries(Configuration conf, HTablePool pool, boolean writeToWAL, RowInterval rowInterval) {
        this.rowInterval = checkNotNull(rowInterval);
        this.pool = checkNotNull(pool);
        this.conf = checkNotNull(conf);
        this.writeToWall = writeToWAL;
    }

    @Override
    public void inc(String type, String key, long timestamp, Map<String, Long> dps) throws IOException {
        this.inc(type, key, new Instant(timestamp), dps);
    }

    public void inc(String type, String key, Instant instant, Map<String, Long> dps) throws IOException {
        long timeStamp = rowInterval.getStart(instant.getMillis());
        byte[] entryKey = getRowKey(key, timeStamp);

        Increment inc = new Increment(entryKey);
        inc.setWriteToWAL(this.writeToWall);
        for (Entry<String, Long> entry : dps.entrySet()) {
            inc.addColumn(CF, Bytes.toBytes(entry.getKey()), entry.getValue());
        }
        HTableInterface hTable = pool.getTable(type);
        try {
            hTable.increment(inc);
        } finally {
            pool.putTable(hTable);
        }
    }

    /**
     * @param type
     * @param key
     * @param interval
     * @return
     * @throws IOException
     */
    @Override
    public Map<String, DataPoints> get(String type, String key, Interval interval) throws IOException {
        ListMultimap<String, DataPoint> dps = ArrayListMultimap.<String, DataPoint> create();
        long start = rowInterval.getStart(interval.getStartMillis());
        long end = interval.getEndMillis();
        byte[] startRow = getStartRowKey(key, start);
        byte[] endRow = getEndRowKey(key, end);
        Scan scan = new Scan(startRow, endRow);
        scan.addFamily(CF);
        scan.setCaching(SCANNER_CACHING);
        ResultScanner results = null;
        HTableInterface hTable = pool.getTable(type);
        try {
            results = hTable.getScanner(scan);
            for (Result result : results) {
                byte[] rowKey = result.getRow();
                long timestamp = getStart(rowKey, key);
                long dpEnd = getEnd(rowKey, key);
                for (KeyValue kv : result.list()) {
                    String metric = Bytes.toString(kv.getQualifier());
                    long value = Bytes.toLong(kv.getValue());
                    if (timestamp >= start && timestamp < end) {
                        dps.put(metric, new DataPoint(timestamp, dpEnd, value));
                    }
                }
            }
        } finally {
            results.close();
            pool.putTable(hTable);
        }
        Map<String, DataPoints> result = newHashMap();
        for (String metric : dps.keySet()) {
            result.put(metric, new WritableDataPoints(key, metric, dps.get(metric), start, end));
        }
        return result;
    }

    @VisibleForTesting
    long getStart(byte[] rowKey, String metricKey) {
        long millis = Bytes.toLong(Arrays.copyOfRange(rowKey, metricKey.length() + 1, rowKey.length));
        return millis;
    }

    @VisibleForTesting
    long getEnd(byte[] rowKey, String metricKey) {
        if (rowKey.length == metricKey.length() + 18) {
            long millis = Bytes.toLong(Arrays.copyOfRange(rowKey, metricKey.length() + 10, rowKey.length));
            return millis;
        } else {
            // Support old keys for now.
            return getStart(rowKey, metricKey) + this.rowInterval.getMillis();
        }

    }

    /**
     * @param key
     * @param instant
     * @return
     */
    @VisibleForTesting
    byte[] getStartRowKey(String key, long instant) {
        byte[] rowKey = new byte[key.length() + 9]; // key + : + timestamp
        System.arraycopy(Bytes.toBytes(key), 0, rowKey, 0, key.length());
        rowKey[key.length()] = ':';
        System.arraycopy(Bytes.toBytes(instant), 0, rowKey, key.length() + 1, 8);
        return rowKey;
    }

    /**
     * Currently same as getStartRowKey
     * 
     * @param key
     * @param instant
     * @return
     */
    byte[] getEndRowKey(String key, long instant) {
        return getStartRowKey(key, instant);
    }

    /**
     * @param key
     * @param instant
     * @return
     */
    @VisibleForTesting
    byte[] getRowKey(String key, long instant) {
        byte[] rowKey = new byte[key.length() + 18]; // key + : + timestamp
        System.arraycopy(Bytes.toBytes(key), 0, rowKey, 0, key.length());
        rowKey[key.length()] = ':';
        long end = instant + rowInterval.getMillis();
        System.arraycopy(Bytes.toBytes(instant), 0, rowKey, key.length() + 1, 8);
        rowKey[key.length() + 9] = ':';
        System.arraycopy(Bytes.toBytes(end), 0, rowKey, key.length() + 10, 8);
        return rowKey;
    }

    @Override
    public void create(String type) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(this.conf);
        HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(type));
        descriptor.addFamily(new HColumnDescriptor(CF));
        admin.createTable(descriptor);
    }

    public boolean checkOrCreateTable(HBaseAdmin admin, String type) throws IOException {
        if (!admin.tableExists(Bytes.toBytes(type))) {
            HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(type));
            descriptor.addFamily(new HColumnDescriptor(CF));
            admin.createTable(descriptor);
            return true;
        } else {
            HTableDescriptor descriptor = admin.getTableDescriptor(toBytes(type));
            boolean modifyRequired = false;
            if (null == descriptor.getFamily(CF)) {
                descriptor.addFamily(new HColumnDescriptor(CF));
                modifyRequired = true;
            }
            if (modifyRequired) {
                admin.disableTable(type);
                admin.modifyTable(toBytes(type), descriptor);
                admin.enableTable(type);
                return true;
            } else {
                return false;
            }
        }
    }

    public void deleteTable(HBaseAdmin admin, String type) throws IOException {
        admin.disableTable(type);
        admin.deleteTable(type);
    }

}
