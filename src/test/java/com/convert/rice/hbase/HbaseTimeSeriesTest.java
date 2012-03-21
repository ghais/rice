/**
 * (C) 2011 Digi-Net Technologies, Inc.
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA
 * All rights reserved.
 */
package com.convert.rice.hbase;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.hadoop.hbase.util.Bytes.toLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Instant;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.convert.rice.AggregationUtility;
import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.protocol.Aggregation;
import com.google.common.collect.Lists;

/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 * 
 */
public class HbaseTimeSeriesTest {

    private static final byte[] CF = HBaseTimeSeries.CF;

    private static HBaseTestingUtility testUtil;

    private static Configuration configuration;

    private static HTablePool pool;

    private static final String type = "type";

    private static HBaseAdmin admin;

    @BeforeClass
    public static void setup() throws Exception {
        testUtil = new HBaseTestingUtility();
        testUtil.startMiniCluster();
        configuration = testUtil.getConfiguration();
        pool = new HTablePool(configuration, 10);
        admin = testUtil.getHBaseAdmin();

        new HBaseTimeSeries(configuration, pool).checkOrCreateTable(admin, type);

    }

    @AfterClass
    public static void after() throws IOException {
        new HBaseTimeSeries(configuration, pool).deleteTable(admin, type);
        pool.close();
        testUtil.shutdownMiniCluster();
    }

    @Test
    public void testInc_1() throws IOException {
        String key = UUID.randomUUID().toString();
        Instant instant = new Instant();
        Instant aggregatedInstant = AggregationUtility.aggregateTo(instant, Aggregation.MINUTE);
        Map<String, Long> dps = new HashMap<String, Long>() {

            private static final long serialVersionUID = -8468159673993092578L;

            {
                put("a", 1L);
                put("b", 2L);
            }
        };
        HBaseTimeSeries ts = new HBaseTimeSeries(configuration, pool, Aggregation.MINUTE);
        ts.inc(type, key, instant.getMillis(), dps);

        HTableInterface table = pool.getTable(type);
        try {
            Get get = new Get(ts.getRowKey(key, aggregatedInstant));
            Result r = table.get(get);
            assertEquals(aggregatedInstant, ts.getInstant(r.getRow(), key));
            assertEquals(1L, toLong(r.getValue(CF, toBytes("a"))));
            assertEquals(2L, toLong(r.getValue(CF, toBytes("b"))));
        } finally {
            pool.putTable(table);
        }
    }

    @Test
    public void testInc_2() throws IOException {
        String key = UUID.randomUUID().toString();
        Map<String, Long> dps = new HashMap<String, Long>() {

            private static final long serialVersionUID = -8468159673993092578L;

            {
                put("a", 1L);
                put("b", 2L);
            }
        };
        HBaseTimeSeries ts = new HBaseTimeSeries(configuration, pool, Aggregation.SECOND);
        for (int i = 1000; i <= 100000; i += 1000) {
            ts.inc(type, key, i, dps);
        }

        HTableInterface table = pool.getTable(type);
        try {
            Scan scan = new Scan(ts.getRowKey(key, new Instant(0)));
            scan.setFilter(new WhileMatchFilter(
                    new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(toBytes(key)))));
            ArrayList<Result> results = Lists.newArrayList(table.getScanner(scan).iterator());
            for (int i = 1; i <= 100; i++) {
                Result r = results.get(i - 1);
                assertEquals(new Instant(i * 1000L), ts.getInstant(r.getRow(), key));
                assertEquals(1L, toLong(r.getValue(CF, toBytes("a"))));
                assertEquals(2L, toLong(r.getValue(CF, toBytes("b"))));
            }
            for (Result r : table.getScanner(scan)) {
                ts.getInstant(r.getRow(), key);
            }
        } finally {
            pool.putTable(table);
        }
    }

    @Test
    public void testInc_3() throws IOException {
        String key = UUID.randomUUID().toString();
        Map<String, Long> dps = new HashMap<String, Long>() {

            private static final long serialVersionUID = -8468159673993092578L;

            {
                put("a", 1L);
                put("b", 2L);
            }
        };
        HBaseTimeSeries ts = new HBaseTimeSeries(configuration, pool, Aggregation.SECOND);
        for (int i = 1000; i <= 100000; i += 1000) {
            ts.inc(type, key, i, dps);
            ts.inc(type, key, i, dps);
        }

        HTableInterface table = pool.getTable(type);
        try {
            Scan scan = new Scan(ts.getRowKey(key, new Instant(0)));
            scan.setFilter(new WhileMatchFilter(
                    new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(toBytes(key)))));
            ArrayList<Result> results = Lists.newArrayList(table.getScanner(scan).iterator());
            for (int i = 1; i <= 100; i++) {
                Result r = results.get(i - 1);
                assertEquals(new Instant(i * 1000L), ts.getInstant(r.getRow(), key));
                assertEquals(2L, toLong(r.getValue(CF, toBytes("a"))));
                assertEquals(4L, toLong(r.getValue(CF, toBytes("b"))));
            }
            for (Result r : table.getScanner(scan)) {
                ts.getInstant(r.getRow(), key);
            }
        } finally {
            pool.putTable(table);
        }
    }

    @Test
    public void testGet_1() throws IOException {
        String key = UUID.randomUUID().toString();
        HBaseTimeSeries ts = new HBaseTimeSeries(configuration, pool);
        Map<String, DataPoints> result = ts.get(type, key, new Interval(0, System.currentTimeMillis()));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGet_2() throws IOException {
        String key = UUID.randomUUID().toString();
        HBaseTimeSeries ts = new HBaseTimeSeries(configuration, pool, Aggregation.SECOND);

        HTableInterface table = pool.getTable(type);
        try {
            for (int i = 1000; i <= 100000; i += 1000) {
                byte[] entryKey = ts.getRowKey(key, new Instant(i));
                Increment inc = new Increment(entryKey);
                inc.addColumn(CF, Bytes.toBytes("a"), 1L);
                inc.addColumn(CF, Bytes.toBytes("b"), 2L);
                table.increment(inc);
            }
        } finally {
            pool.putTable(table);
        }

        Map<String, DataPoints> result = ts.get(type, key, new Interval(1000, 100001));
        assertEquals(2, result.size());
        DataPoints as = result.get("a");
        DataPoints bs = result.get("b");
        assertEquals(100, as.size());
        for (int i = 0; i < 100; i++) {
            DataPoint dp = as.get(i);
            assertEquals((i + 1) * 1000, dp.getTimestamp());
            assertEquals(1, dp.getValue());
        }

        assertEquals(100, bs.size());
        for (int i = 0; i < 100; i++) {
            DataPoint dp = bs.get(i);
            assertEquals((i + 1) * 1000, dp.getTimestamp());
            assertEquals(2, dp.getValue());
        }
    }

    @Test
    public void testCreate() throws IOException {
        new HBaseTimeSeries(configuration, pool).create("u");

        HTableDescriptor tableDescriptor = admin.getTableDescriptor(new byte[] { 'u' });
        assertEquals(1, tableDescriptor.getFamiliesKeys().size());
        assertTrue(tableDescriptor.getFamiliesKeys().contains(CF));
    }

    @Test(expected = TableExistsException.class)
    public void testCreate_table_exists() throws IOException {
        HTableDescriptor descriptor = new HTableDescriptor(new byte[] { 'u' });
        descriptor.addFamily(new HColumnDescriptor(new byte[] { 'x' }));
        admin.createTable(descriptor);

        new HBaseTimeSeries(configuration, pool).create("u");
    }
}
