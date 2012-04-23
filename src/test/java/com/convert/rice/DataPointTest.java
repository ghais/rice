package com.convert.rice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * The class <code>DataPointTest</code> contains tests for the class <code>{@link DataPoint}</code>.
 * 
 */
public class DataPointTest {

    /**
     * Run the DataPoint(long,long) constructor test.
     * 
     */
    @Test
    public void testDataPoint_1() {
        long value = 1L;
        long start = 1L;
        long end = 2L;

        DataPoint result = new DataPoint(start, end, value);

        // add additional test code here
        assertNotNull(result);
        assertEquals(1L, result.getValue());
        assertEquals(1L, result.getStart());
        assertEquals(2L, result.getEnd());
    }

    /**
     * Run the boolean equals(Object) method test.
     */
    @Test
    public void testEquals_1() {
        DataPoint fixture = new DataPoint(1L, 1L, 1L);
        Object obj = new DataPoint(1L, 1L, 1L);

        boolean result = fixture.equals(obj);

        assertEquals(true, result);
    }

    /**
     * Run the boolean equals(Object) method test.
     */
    @Test
    public void testEquals_2() {
        DataPoint fixture = new DataPoint(1L, 1L, 1L);
        Object obj = null;

        boolean result = fixture.equals(obj);

        assertEquals(false, result);
    }

    /**
     * Run the boolean equals(Object) method test.
     * 
     */
    @Test
    public void testEquals_3() {
        DataPoint fixture = new DataPoint(1L, 1L, 1L);
        Object obj = new Object();

        boolean result = fixture.equals(obj);

        assertEquals(false, result);
    }

    /**
     * Run the boolean equals(Object) method test.
     * 
     */
    @Test
    public void testEquals_4() {
        DataPoint fixture = new DataPoint(1L, 1L, 1L);
        Object obj = new DataPoint(1L, 1L, 1L);

        boolean result = fixture.equals(obj);

        assertEquals(true, result);
    }

    /**
     * Run the boolean equals(Object) method test.
     * 
     */
    @Test
    public void testEquals_5() {
        DataPoint fixture = new DataPoint(1L, 1L, 1L);
        Object obj = new DataPoint(1L, 1L, 1L);

        boolean result = fixture.equals(obj);

        assertEquals(true, result);
    }

    /**
     * Run the boolean equals(Object) method test.
     */
    @Test
    public void testEquals_6() {
        DataPoint fixture = new DataPoint(1L, 1L, 1L);
        Object obj = new DataPoint(1L, 1L, 1L);

        boolean result = fixture.equals(obj);

        assertEquals(true, result);
    }

    /**
     * Run the long getTimestamp() method test.
     * 
     */
    @Test
    public void testGetTimestamp_1() {
        DataPoint fixture = new DataPoint(1L, 1L, 1L);

        long result = fixture.getStart();

        assertEquals(1L, result);
    }

    /**
     * Run the long getValue() method test.
     * 
     */
    @Test
    public void testGetValue_1() {
        DataPoint fixture = new DataPoint(1L, 1L, 1L);

        long result = fixture.getValue();

        assertEquals(1L, result);
    }

    /**
     * Run the int hashCode() method test.
     * 
     */
    @Test
    public void testHashCode_1() {
        DataPoint fixture = new DataPoint(1L, 1L, 1L);

        int result = fixture.hashCode();

        // add additional test code here
        assertEquals(30784, result);
    }

}
