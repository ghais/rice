package com.convert.rice.functions.map;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.WritableDataPoints;
import com.convert.rice.protocol.MapReduce.GroupBy;

public class GroupByTest {

    private static final String KEY = "key";

    private static final String METRIC_NAME = "name";

    @Test
    public void testGroupBy_1() {
        DataPoints dataPoints = new WritableDataPoints(KEY, METRIC_NAME, 0, 10000);
        List<DataPoints> spans = new GroupByFunc(GroupBy.newBuilder().setStep(1000).build()).apply(dataPoints);
        assertEquals(10, spans.size());
        for (int i = 0; i < 10; i++) {
            DataPoints dps = spans.get(i);
            long start = dps.getStart();
            long end = dps.getEnd();
            assertEquals(i * 1000, start);
            assertEquals((i + 1) * 1000, end);
            assertEquals(0, dps.size());
        }
    }

    @Test
    public void testGroupBy_2() {
        DataPoints dataPoints = new WritableDataPoints(KEY, METRIC_NAME, 0, 10500);
        List<DataPoints> spans = new GroupByFunc(GroupBy.newBuilder().setStep(1000).build()).apply(dataPoints);
        assertEquals(11, spans.size());
        for (int i = 0; i < 11; i++) {
            DataPoints dps = spans.get(i);
            long start = dps.getStart();
            long end = dps.getEnd();
            assertEquals(i * 1000, start);
            assertEquals((i + 1) * 1000, end);
            assertEquals(0, dps.size());
        }
    }

    @Test
    public void testGroupBy_3() {
        List<DataPoint> points = new ArrayList<DataPoint>() {

            private static final long serialVersionUID = 1L;

            {
                add(new DataPoint(0, 1000, 0));
                add(new DataPoint(1000, 2000, 1));
                add(new DataPoint(2000, 3000, 2));
                add(new DataPoint(3000, 4000, 3));
                add(new DataPoint(4000, 5000, 4));
                add(new DataPoint(5000, 6000, 5));
                add(new DataPoint(6000, 7000, 6));
                add(new DataPoint(7000, 8000, 7));
                add(new DataPoint(8000, 9000, 8));
                add(new DataPoint(9000, 10000, 9));
                add(new DataPoint(10000, 11000, 10));

            }
        };
        DataPoints dataPoints = new WritableDataPoints(KEY, METRIC_NAME, points, 0, 10500);
        GroupBy groupBy = GroupBy.newBuilder().setStep(1000).build();
        List<DataPoints> spans = new GroupByFunc(groupBy).apply(dataPoints);
        assertEquals(11, spans.size());
        for (int i = 0; i < 11; i++) {
            DataPoints dps = spans.get(i);
            long start = dps.getStart();
            long end = dps.getEnd();
            assertEquals(i * 1000, start);
            assertEquals((i + 1) * 1000, end);
            assertEquals(1, dps.size());
            assertEquals(i, dps.get(0).getValue());
        }
    }

    @Test
    public void testGroupBy_4() {
        List<DataPoint> points = new ArrayList<DataPoint>() {

            private static final long serialVersionUID = 1L;

            {
                add(new DataPoint(0, 1000, 0));
                add(new DataPoint(500, 1000, 0));

                add(new DataPoint(1000, 2000, 1));
                add(new DataPoint(1500, 2000, 1));

                add(new DataPoint(2000, 3000, 2));
                add(new DataPoint(2500, 3000, 2));

                add(new DataPoint(3000, 4000, 3));
                add(new DataPoint(3500, 4000, 3));

                add(new DataPoint(4000, 5000, 4));
                add(new DataPoint(4500, 5000, 4));

                add(new DataPoint(5000, 6000, 5));
                add(new DataPoint(5500, 6000, 5));

                add(new DataPoint(6000, 7000, 6));
                add(new DataPoint(6500, 7000, 6));

                add(new DataPoint(7000, 8000, 7));
                add(new DataPoint(7500, 8000, 7));

                add(new DataPoint(8000, 9000, 8));
                add(new DataPoint(8500, 9000, 8));

                add(new DataPoint(9000, 10000, 9));
                add(new DataPoint(9500, 10000, 9));

                add(new DataPoint(10000, 11000, 10));
                add(new DataPoint(10500, 11000, 10));
            }
        };
        DataPoints dataPoints = new WritableDataPoints(KEY, METRIC_NAME, points, 0, 10500);
        List<DataPoints> spans = new GroupByFunc(GroupBy.newBuilder().setStep(1000).build()).apply(dataPoints);
        assertEquals(11, spans.size());
        for (int i = 0; i < 11; i++) {
            DataPoints dps = spans.get(i);
            long start = dps.getStart();
            long end = dps.getEnd();
            assertEquals(i * 1000, start);
            assertEquals((i + 1) * 1000, end);
            assertEquals(2, dps.size());
            assertEquals(i, dps.get(0).getValue());
            assertEquals(i, dps.get(1).getValue());
        }
    }
}
