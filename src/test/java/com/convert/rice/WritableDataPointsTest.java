/**
 * (C) 2011 Digi-Net Technologies, Inc.
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA
 * All rights reserved.
 */
package com.convert.rice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Test;

import com.convert.rice.protocol.Aggregation;

/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 * 
 */
public class WritableDataPointsTest {

    private static final String KEY = "key";

    private static final String METRIC_NAME = "name";

    /**
     * Tests that when the datapoints are empty aggregation returns an empty map.
     */
    @Test
    public void testAggregate_1() {
        WritableDataPoints dps = new WritableDataPoints(KEY, METRIC_NAME, new ArrayList<DataPoint>(0), 0, 0);
        assertTrue(dps.aggregate(Aggregation.HOUR).isEmpty());
        assertTrue(dps.aggregate(Aggregation.DAY).isEmpty());
        assertTrue(dps.aggregate(Aggregation.MONTH).isEmpty());
    }

    /**
     * Test that when the datapoints are already aggregated to a certain format, re-aggregating them to the same unit
     * has no effect.
     */
    @Test
    public void testAggregate_2() {
        final Instant instant1 = new DateTime(2012, 01, 01, 00, 00).toInstant();
        final Instant instant2 = new DateTime(2012, 01, 02, 00, 00).toInstant();
        final Instant instant3 = new DateTime(2012, 01, 03, 00, 00).toInstant();

        ArrayList<DataPoint> dps = new ArrayList<DataPoint>(10) {

            private static final long serialVersionUID = 6418821022217331787L;

            {
                add(new DataPoint(100, instant1.getMillis()));
                add(new DataPoint(200, instant2.getMillis()));
                add(new DataPoint(300, instant3.getMillis()));

            }
        };

        // Add 1 to the end time stamp to make the last instant inclusive
        SortedMap<Long, Long> result = new WritableDataPoints(KEY, METRIC_NAME, dps, instant1.getMillis(),
                instant3.getMillis() + 1).aggregate(Aggregation.HOUR);
        Iterator<Entry<Long, Long>> iterator = result.entrySet().iterator();
        Instant current = instant1;
        while (iterator.hasNext()) {
            Entry<Long, Long> entry = iterator.next();
            assertEquals(current.getMillis(), entry.getKey().longValue());
            if (entry.getKey().longValue() == instant1.getMillis()) {
                assertEquals(100, entry.getValue().longValue());
            } else if (entry.getKey().longValue() == instant2.getMillis()) {
                assertEquals(200, entry.getValue().longValue());
            } else if (entry.getKey().longValue() == instant3.getMillis()) {
                assertEquals(300, entry.getValue().longValue());
            } else {
                assertEquals(0L, entry.getValue().longValue());
            }

            current = current.plus(3600 * 1000);
        }
    }

    /**
     * Test that aggregating from hour to day works as expected.
     */
    @Test
    public void testAggregate_3() {
        final Instant instant1 = new DateTime(2012, 01, 01, 00, 00, DateTimeZone.UTC).toInstant();
        final Instant instant2 = new DateTime(2012, 01, 01, 02, 01, DateTimeZone.UTC).toInstant();
        final Instant instant3 = new DateTime(2012, 02, 03, 00, 00, DateTimeZone.UTC).toInstant();

        ArrayList<DataPoint> dps = new ArrayList<DataPoint>(10) {

            private static final long serialVersionUID = 6418821022217331787L;

            {
                add(new DataPoint(100, instant1.getMillis()));
                add(new DataPoint(200, instant2.getMillis()));
                add(new DataPoint(300, instant3.getMillis()));

            }
        };

        // Add 1 to the end time stamp to make the last instant inclusive
        SortedMap<Long, Long> result = new WritableDataPoints(KEY, METRIC_NAME, dps, instant1.getMillis(),
                instant3.getMillis() + 1).aggregate(Aggregation.DAY);
        Iterator<Entry<Long, Long>> iterator = result.entrySet().iterator();
        Instant current = instant1;
        while (iterator.hasNext()) {
            Entry<Long, Long> entry = iterator.next();
            assertEquals(current.getMillis(), entry.getKey().longValue());
            if (entry.getKey().longValue() == instant1.getMillis()) {
                // We aggregated instant1 and instant2
                assertEquals(300, entry.getValue().longValue());
            } else if (entry.getKey().longValue() == instant3.getMillis()) {
                assertEquals(300, entry.getValue().longValue());
            } else {
                assertEquals(0L, entry.getValue().longValue());
            }

            current = current.plus(3600 * 1000 * 24);
        }
    }
}
