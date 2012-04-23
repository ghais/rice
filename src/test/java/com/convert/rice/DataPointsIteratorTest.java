package com.convert.rice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.NoSuchElementException;

import org.junit.Test;

public class DataPointsIteratorTest {

    /**
     * Run the DataPointsIterator(DataPoints) constructor test.
     *
     */
    @Test
    public void testDataPointsIterator_1()
            throws Exception {
        DataPoints dataPoints = new WritableDataPoints("", "", 1L, 1L);

        DataPointsIterator result = new DataPointsIterator(dataPoints);

        // add additional test code here
        assertNotNull(result);
    }

    @Test
    public void testHasNext_1()
            throws Exception {
        DataPointsIterator fixture = new DataPointsIterator(new WritableDataPoints("", "", 1L, 1L));

        boolean result = fixture.hasNext();
        assertEquals(false, result);
    }

    @Test
    public void testIterator_1()
            throws Exception {
        WritableDataPoints dataPoints = new WritableDataPoints("", "", 0L, 1L);
        DataPoint dp = new DataPoint(0, 1, 1);
        dataPoints.add(dp);
        DataPointsIterator iterator = new DataPointsIterator(dataPoints);

        assertTrue(iterator.hasNext());
        assertEquals(dp, iterator.next());

        assertFalse(iterator.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void testNext_1()
            throws Exception {
        DataPointsIterator iterator = new DataPointsIterator(new WritableDataPoints("", "", 1L, 1L));

        iterator.next();
    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testRemove_1()
            throws Exception {
        DataPointsIterator fixture = new DataPointsIterator(new WritableDataPoints("", "", 1L, 1L));

        fixture.remove();

    }

    @Test(expected = NoSuchElementException.class)
    public void testTimestamp_1()
            throws Exception {
        DataPointsIterator fixture = new DataPointsIterator(new WritableDataPoints("", "", 1L, 1L));

        fixture.timestamp();
    }

    public void testTimestamp_2()
            throws Exception {
        WritableDataPoints dataPoints = new WritableDataPoints("", "", 0L, 1L);
        dataPoints.add(new DataPoint(0, 1, 1));
        DataPointsIterator fixture = new DataPointsIterator(dataPoints);

        long result = fixture.timestamp();
        assertEquals(0L, result);
    }

    @Test(expected = NoSuchElementException.class)
    public void testValue_1()
            throws Exception {
        DataPointsIterator fixture = new DataPointsIterator(new WritableDataPoints("", "", 1L, 1L));

        fixture.value();
    }

    @Test
    public void testValue_2()
            throws Exception {
        WritableDataPoints dataPoints = new WritableDataPoints("", "", 0L, 1L);
        dataPoints.add(new DataPoint(0, 1, 1));
        DataPointsIterator fixture = new DataPointsIterator(dataPoints);

        long result = fixture.value();
        assertEquals(1L, result);
    }

    @Test
    public void testSeek_1() {

    }

}
