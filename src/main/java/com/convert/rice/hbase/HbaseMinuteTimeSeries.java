package com.convert.rice.hbase;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.TimeSeries;
import com.convert.rice.WritableDataPoints;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class HbaseMinuteTimeSeries implements TimeSeries {

    public static final DateTimeFormatter FORMATER = DateTimeFormat.forPattern("yyyyMMddHHmm").withZoneUTC();;

    public static final byte[] DEFAULT_FAMILY = new byte[] { 'v' };

    private static final int DAY = 24 * 60;

    private final HTablePool pool;

    /**
     * The HTable associated with the metric type.
     * 
     * @param hTable
     */
    public HbaseMinuteTimeSeries(HTablePool pool) {
        this.pool = pool;
    }

    public void inc(String type, String key, Map<String, Long> dps) throws IOException {
        this.inc(type, key, new DateTime(DateTimeZone.UTC), dps);
    }

    @Override
    public void inc(String type, String key, long timestamp, Map<String, Long> dps) throws IOException {
        this.inc(type, key, new DateTime(timestamp, DateTimeZone.UTC), dps);
    }

    public void inc(String type, String key, DateTime timeStamp, Map<String, Long> dps) throws IOException {
        String entryKey = getRowKey(key, timeStamp);
        Increment inc = new Increment(Bytes.toBytes(entryKey));
        for (Entry<String, Long> entry : dps.entrySet()) {
            inc.addColumn(DEFAULT_FAMILY, toBytes(entry.getKey()), entry.getValue());
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
    public Collection<DataPoints> get(String type, String key, Interval interval, Collection<String> metrics)
            throws IOException {
        Period period = interval.toPeriod();
        ListMultimap<String, DataPoint> dps = ArrayListMultimap.<String, DataPoint> create();

        DateTime start = interval.getStart();
        DateTime end = interval.getEnd().plusHours(1); // we add one minute since the scan is exclusive.
        String startRow = getRowKey(key, start);
        String endRow = getRowKey(key, end);
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
        scan.addFamily(DEFAULT_FAMILY);
        scan.setCaching(Math.min(DAY, period.getHours()));

        ResultScanner results = null;
        HTableInterface hTable = pool.getTable(type);
        try {
            results = hTable.getScanner(scan);
        } finally {
            pool.putTable(hTable);
        }
        for (Result result : results) {
            String rowKey = Bytes.toString(result.getRow());
            String[] split = StringUtils.split(rowKey, ":::");
            DateTime minute = FORMATER.parseDateTime(split[split.length - 1]);
            for (KeyValue kv : result.list()) {
                String metric = Bytes.toString(kv.getQualifier());
                if (metrics.isEmpty() || metric.contains(metric)) {
                    long value = Bytes.toLong(kv.getValue());
                    long timestamp = minute.getMillis();
                    if (timestamp >= start.getMillis() && timestamp < end.getMillis()) {
                        dps.put(metric, new DataPoint(value, timestamp));
                    }
                }
            }
        }
        List<DataPoints> result = new ArrayList<DataPoints>(dps.size());
        for (String metric : dps.keySet()) {
            result.add(new WritableDataPoints(key, metric, dps.get(metric)));
        }
        return result;
    }

    /**
     * @param key
     * @param cal
     * @return
     */
    private String getRowKey(String key, DateTime date) {
        return key + ":::" + FORMATER.print(date);
    }

    public static boolean checkOrCreateTable(HBaseAdmin admin, String type, String... metrics) throws IOException {
        if (!admin.tableExists(Bytes.toBytes(type))) {
            HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(type));
            for (String metric : metrics) {
                descriptor.addFamily(new HColumnDescriptor(toBytes(metric)));
            }
            admin.createTable(descriptor);
            return true;
        } else {
            HTableDescriptor descriptor = admin.getTableDescriptor(toBytes(type));
            boolean modifyRequired = false;
            for (String metric : metrics) {
                if (null == descriptor.getFamily(toBytes(metric))) {
                    descriptor.addFamily(new HColumnDescriptor(toBytes(metric)));
                    modifyRequired = true;
                }
                if (modifyRequired) {
                    admin.disableTable(type);
                    admin.modifyTable(toBytes(type), descriptor);
                    admin.enableTable(type);
                }
            }
            return false;
        }
    }
}
