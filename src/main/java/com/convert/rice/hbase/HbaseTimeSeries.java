package com.convert.rice.hbase;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

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
import org.joda.time.Period;
import org.joda.time.PeriodType;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.DownSampleUtility;
import com.convert.rice.TimeSeries;
import com.convert.rice.WritableDataPoints;
import com.convert.rice.protocol.DownSample;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class HbaseTimeSeries implements TimeSeries {

    /**
     * 
     */
    private static final int DEFAULT_CACHING = 60 * 24;

    public static final byte[] DEFAULT_FAMILY = new byte[] { 'v' };

    private final HTablePool pool;

    /**
     * The HTable associated with the metric type.
     * 
     * @param hTable
     */
    public HbaseTimeSeries(HTablePool pool) {
        this.pool = pool;
    }

    public void inc(String type, String key, Map<String, Long> dps) throws IOException {
        this.inc(type, key, new Instant(), dps);
    }

    @Override
    public void inc(String type, String key, long timestamp, Map<String, Long> dps, DownSample downSample)
            throws IOException {
        this.inc(type, key, DownSampleUtility.downSample(new Instant(timestamp), downSample), dps);
    }

    public void inc(String type, String key, Instant timeStamp, Map<String, Long> dps) throws IOException {
        byte[] entryKey = getRowKey(key, timeStamp);

        Increment inc = new Increment(entryKey);
        for (Entry<String, Long> entry : dps.entrySet()) {
            inc.addColumn(DEFAULT_FAMILY, Bytes.toBytes(entry.getKey()), entry.getValue());
        }
        HTableInterface hTable = pool.getTable(type);
        try {
            hTable.increment(inc);
        } finally {
            pool.putTable(hTable);
        }
    }

    /**
     * 
     * @param key
     * @param interval
     * @return
     * @throws IOException
     */
    @Override
    public Collection<DataPoints> get(String type, String key, Interval interval) throws IOException {
        Period period = interval.toPeriod(PeriodType.hours());
        ListMultimap<String, DataPoint> dps = ArrayListMultimap.<String, DataPoint> create();

        Instant start = interval.getStart().toInstant();
        Instant end = interval.getEnd().toInstant();
        byte[] startRow = getRowKey(key, start);
        byte[] endRow = getRowKey(key, end);
        Scan scan = new Scan(startRow, endRow);
        scan.addFamily(DEFAULT_FAMILY);
        scan.setCaching(Math.min(DEFAULT_CACHING, period.getMinutes()));
        ResultScanner results = null;
        HTableInterface hTable = pool.getTable(type);
        try {
            results = hTable.getScanner(scan);
        } finally {
            pool.putTable(hTable);
        }
        for (Result result : results) {
            byte[] rowKey = result.getRow();
            Instant instant = getInstant(rowKey, key);

            for (KeyValue kv : result.list()) {
                String metric = Bytes.toString(kv.getQualifier());
                long value = Bytes.toLong(kv.getValue());
                long timestamp = instant.getMillis();
                if (timestamp >= start.getMillis() && timestamp < end.getMillis()) {
                    dps.put(metric, new DataPoint(value, timestamp));
                }
            }
        }
        List<DataPoints> result = new ArrayList<DataPoints>(dps.size());
        for (String metric : dps.keySet()) {
            result.add(new WritableDataPoints(key, metric, dps.get(metric)));
        }
        return result;
    }

    @VisibleForTesting
    Instant getInstant(byte[] rowKey, String metricKey) {
        long millis = Bytes.toLong(Arrays.copyOfRange(rowKey, metricKey.length() + 1, rowKey.length));
        return new Instant(millis);
    }

    /**
     * @param key
     * @param instant
     * @return
     */
    @VisibleForTesting
    byte[] getRowKey(String key, Instant instant) {
        byte[] rowKey = new byte[key.length() + 1 + 8]; // key + : + timestamp
        System.arraycopy(Bytes.toBytes(key), 0, rowKey, 0, key.length());
        rowKey[key.length()] = ':';
        System.arraycopy(Bytes.toBytes(instant.getMillis()), 0, rowKey, key.length() + 1, 8);
        return rowKey;
    }

    public boolean checkOrCreateTable(HBaseAdmin admin, String type, String... metrics) throws IOException {
        if (!admin.tableExists(Bytes.toBytes(type))) {
            HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(type));
            descriptor.addFamily(new HColumnDescriptor(DEFAULT_FAMILY));
            admin.createTable(descriptor);
            return true;
        } else {
            HTableDescriptor descriptor = admin.getTableDescriptor(toBytes(type));
            boolean modifyRequired = false;
            if (null == descriptor.getFamily(DEFAULT_FAMILY)) {
                descriptor.addFamily(new HColumnDescriptor(DEFAULT_FAMILY));
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

    public static void main(String[] args) throws IOException {
        HbaseTimeSeries ts = new HbaseTimeSeries(new HTablePool());
        Instant date = new Instant();
        System.out.println(date);
        byte[] rowKey = ts.getRowKey(UUID.randomUUID().toString(), date);
        System.out.println(new String(rowKey));
        System.out.println(ts.getInstant(rowKey, UUID.randomUUID().toString()));
    }
}
