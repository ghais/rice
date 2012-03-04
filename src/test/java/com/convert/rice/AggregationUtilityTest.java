/**
 * (C) 2011 Digi-Net Technologies, Inc.
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA
 * All rights reserved.
 */
package com.convert.rice;

import static org.joda.time.DateTimeFieldType.dayOfMonth;
import static org.joda.time.DateTimeFieldType.hourOfDay;
import static org.joda.time.DateTimeFieldType.millisOfSecond;
import static org.joda.time.DateTimeFieldType.minuteOfHour;
import static org.joda.time.DateTimeFieldType.monthOfYear;
import static org.joda.time.DateTimeFieldType.secondOfMinute;
import static org.joda.time.DateTimeFieldType.year;
import static org.junit.Assert.assertEquals;

import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;

import com.convert.rice.protocol.Aggregation;

/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 * 
 */
public class AggregationUtilityTest {

    @Test
    public void testMinute() {
        Instant original = new Instant();
        Instant instant = AggregationUtility.aggregateTo(original, Aggregation.MINUTE);
        assertEquals(0L, instant.get(millisOfSecond()));
        assertEquals(0L, instant.get(secondOfMinute()));
        assertEquals(original.get(minuteOfHour()), instant.get(minuteOfHour()));
        assertEquals(original.get(hourOfDay()), instant.get(hourOfDay()));
        assertEquals(original.get(dayOfMonth()), instant.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), instant.get(monthOfYear()));
        assertEquals(original.get(year()), instant.get(year()));
    }

    @Test
    public void testHour() {
        Instant original = new Instant();
        Instant instant = AggregationUtility.aggregateTo(original, Aggregation.HOUR);
        assertEquals(0L, instant.get(millisOfSecond()));
        assertEquals(0L, instant.get(secondOfMinute()));
        assertEquals(0L, instant.get(minuteOfHour()));
        assertEquals(original.get(hourOfDay()), instant.get(hourOfDay()));
        assertEquals(original.get(dayOfMonth()), instant.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), instant.get(monthOfYear()));
        assertEquals(original.get(year()), instant.get(year()));
    }

    @Test
    public void testDay() {
        Instant original = new Instant();
        Instant instant = AggregationUtility.aggregateTo(original, Aggregation.DAY);
        assertEquals(0L, instant.get(millisOfSecond()));
        assertEquals(0L, instant.get(secondOfMinute()));
        assertEquals(0L, instant.get(minuteOfHour()));
        assertEquals(0L, instant.get(hourOfDay()));
        assertEquals(original.get(dayOfMonth()), instant.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), instant.get(monthOfYear()));
        assertEquals(original.get(year()), instant.get(year()));

    }

    @Test
    public void testMonth() {
        Instant original = new Instant();
        Instant instant = AggregationUtility.aggregateTo(original, Aggregation.MONTH);
        assertEquals(0L, instant.get(millisOfSecond()));
        assertEquals(0L, instant.get(secondOfMinute()));
        assertEquals(0L, instant.get(minuteOfHour()));
        assertEquals(0L, instant.get(hourOfDay()));
        assertEquals(1L, instant.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), instant.get(monthOfYear()));
        assertEquals(original.get(year()), instant.get(year()));
    }

    @Test
    public void testMonth_2() {
        Instant original = new Instant(new DateTime(1920, 2, 1, 3, 10, 31, 100));
        Instant instant = AggregationUtility.aggregateTo(original, Aggregation.MONTH);
        assertEquals(0L, instant.get(millisOfSecond()));
        assertEquals(0L, instant.get(secondOfMinute()));
        assertEquals(0L, instant.get(minuteOfHour()));
        assertEquals(0L, instant.get(hourOfDay()));
        assertEquals(1L, instant.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), instant.get(monthOfYear()));
        assertEquals(original.get(year()), instant.get(year()));
    }
}
