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
import static org.joda.time.PeriodType.hours;
import static org.joda.time.PeriodType.minutes;
import static org.joda.time.PeriodType.seconds;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.Period;
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

    @Test
    public void testEmptyMap_1() {
        DateTime start = new DateTime(2012, 01, 01, 00, 00, 00, 00, getInstanceUTC());
        DateTime end = new DateTime(2012, 01, 02, 00, 00, 00, 00, getInstanceUTC());

        Map<Long, Long> result = AggregationUtility.newMap(start.getMillis(), end.getMillis(), Aggregation.SECOND);
        assertEquals(new Period(start, end, seconds()).getSeconds(), result.size());
    }

    @Test
    public void testEmptyMap_2() {
        DateTime start = new DateTime(2012, 01, 01, 00, 00, 00, 00, getInstanceUTC());
        DateTime end = new DateTime(2012, 01, 02, 00, 00, 00, 00, getInstanceUTC());

        Map<Long, Long> result = AggregationUtility.newMap(start.getMillis(), end.getMillis(), Aggregation.MINUTE);
        assertEquals(new Period(start, end, minutes()).getMinutes(), result.size());
    }

    @Test
    public void testEmptyMap_3() {
        DateTime start = new DateTime(2012, 01, 01, 00, 00, 00, 00, getInstanceUTC());
        DateTime end = new DateTime(2012, 01, 02, 00, 00, 00, 00, getInstanceUTC());

        Map<Long, Long> result = AggregationUtility.newMap(start.getMillis(), end.getMillis(), Aggregation.HOUR);
        assertEquals(new Period(start, end, hours()).getHours(), result.size());
    }

    @Test
    public void testEmptyMap_4() {
        DateTime start = new DateTime(2012, 01, 01, 00, 00, 00, 00, getInstanceUTC());
        DateTime end = new DateTime(2012, 01, 02, 05, 00, 00, 00, getInstanceUTC());

        Map<Long, Long> result = AggregationUtility.newMap(start.getMillis(), end.getMillis(), Aggregation.DAY);
        assertEquals(2, result.size()); // one for 2012/01/01 and one for 2012/01/02
    }

    @Test
    public void testEmptyMap_5() {
        DateTime start = new DateTime(2012, 01, 01, 00, 00, 00, 00, getInstanceUTC());
        DateTime end = new DateTime(2012, 02, 02, 00, 00, 00, 00, getInstanceUTC());

        Map<Long, Long> result = AggregationUtility.newMap(start.getMillis(), end.getMillis(), Aggregation.MONTH);
        assertEquals(2, result.size()); // one for 2012/01/01 and one for 2012/02/02
    }
}
