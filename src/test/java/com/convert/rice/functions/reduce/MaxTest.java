package com.convert.rice.functions.reduce;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.convert.rice.DataPoint;
import com.convert.rice.WritableDataPoints;

public class MaxTest {

    @Test
    public void testMax_1() {
        WritableDataPoints dataPoints = new WritableDataPoints("k", "m", 0, 1000);
        DataPoint dp = new Max().apply(dataPoints);
        assertEquals(dataPoints.getStart(), dp.getStart());
        assertEquals(dataPoints.getEnd(), dp.getEnd());
        assertEquals(0, dp.getValue());
    }

    @Test
    public void testMax_2() {

        List<DataPoint> points = new ArrayList<DataPoint>() {

            private static final long serialVersionUID = 1L;

            {
                add(new DataPoint(0, 1000, 1));
            }
        };

        WritableDataPoints dataPoints = new WritableDataPoints("k", "m", points, 0, 1000);
        DataPoint dp = new Max().apply(dataPoints);
        assertEquals(dataPoints.getStart(), dp.getStart());
        assertEquals(dataPoints.getEnd(), dp.getEnd());
        assertEquals(1, dp.getValue());
    }

    @Test
    public void testMax_3() {

        List<DataPoint> points = new ArrayList<DataPoint>() {

            private static final long serialVersionUID = 1L;

            {
                add(new DataPoint(0, 1000, 1));
                add(new DataPoint(500, 1000, 1));
            }
        };

        WritableDataPoints dataPoints = new WritableDataPoints("k", "m", points, 0, 1000);
        DataPoint dp = new Max().apply(dataPoints);
        assertEquals(dataPoints.getStart(), dp.getStart());
        assertEquals(dataPoints.getEnd(), dp.getEnd());
        assertEquals(1, dp.getValue());
    }

    @Test
    public void testMax_4() {

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

        WritableDataPoints dataPoints = new WritableDataPoints("k", "m", points, 0, 1000);
        DataPoint dp = new Max().apply(dataPoints);
        assertEquals(dataPoints.getStart(), dp.getStart());
        assertEquals(dataPoints.getEnd(), dp.getEnd());
        assertEquals(10, dp.getValue());
    }

    @Test
    public void testMax_5() {

        List<DataPoint> points = new ArrayList<DataPoint>() {

            private static final long serialVersionUID = 1L;

            {
                add(new DataPoint(0, 1000, 10));
                add(new DataPoint(500, 1000, 10));

                add(new DataPoint(1000, 2000, 9));
                add(new DataPoint(1500, 2000, 9));

                add(new DataPoint(2000, 3000, 1));
                add(new DataPoint(2500, 3000, 1));

            }
        };

        WritableDataPoints dataPoints = new WritableDataPoints("k", "m", points, 0, 1000);
        DataPoint dp = new Max().apply(dataPoints);
        assertEquals(dataPoints.getStart(), dp.getStart());
        assertEquals(dataPoints.getEnd(), dp.getEnd());
        assertEquals(10, dp.getValue());
    }

    @Test
    public void testMax_6() {

        List<DataPoint> points = new ArrayList<DataPoint>() {

            private static final long serialVersionUID = 1L;

            {
                add(new DataPoint(0, 1000, 10));
                add(new DataPoint(500, 1000, -10));

                add(new DataPoint(1000, 2000, 9));
                add(new DataPoint(1500, 2000, 90));

                add(new DataPoint(2000, 3000, 1));
                add(new DataPoint(2500, 3000, 1));

            }
        };

        WritableDataPoints dataPoints = new WritableDataPoints("k", "m", points, 0, 1000);
        DataPoint dp = new Max().apply(dataPoints);
        assertEquals(dataPoints.getStart(), dp.getStart());
        assertEquals(dataPoints.getEnd(), dp.getEnd());
        assertEquals(90, dp.getValue());
    }
}
