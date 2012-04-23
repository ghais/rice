package com.convert.rice;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class WritableDataPointsTest {

    private static final String KEY = "key";

    private static final String METRIC_NAME = "name";

    @Test
    public void testTimestamp_1() {
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
        assertEquals(0, dataPoints.timestamp(0));
        assertEquals(500, dataPoints.timestamp(1));
        assertEquals(1000, dataPoints.timestamp(2));
        assertEquals(1500, dataPoints.timestamp(3));
        assertEquals(2000, dataPoints.timestamp(4));
        assertEquals(10000, dataPoints.timestamp(20));
        assertEquals(10500, dataPoints.timestamp(21));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAdd_1() {
        WritableDataPoints dataPoints = new WritableDataPoints(KEY, METRIC_NAME, 0, 10500);
        dataPoints.add(new DataPoint(-1, Long.MAX_VALUE, 10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAdd_2() {
        WritableDataPoints dataPoints = new WritableDataPoints(KEY, METRIC_NAME, 0, 10500);
        dataPoints.add(new DataPoint(1000, 10500, 10));
        dataPoints.add(new DataPoint(0, 10500, 10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAdd_3() {
        WritableDataPoints dataPoints = new WritableDataPoints(KEY, METRIC_NAME, 0, 10500);
        dataPoints.add(new DataPoint(0, 1000, 10));
        dataPoints.add(new DataPoint(1000, 2000, 10));
        dataPoints.add(new DataPoint(0, 1000, 10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAdd_4() {
        WritableDataPoints dataPoints = new WritableDataPoints(KEY, METRIC_NAME, 0, 10500);
        dataPoints.add(new DataPoint(0, 100000, 10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAdd_5() {
        WritableDataPoints dataPoints = new WritableDataPoints(KEY, METRIC_NAME, 0, 10500);
        dataPoints.add(new DataPoint(100000, Long.MAX_VALUE, 10));
    }

}
