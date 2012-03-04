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

import com.convert.rice.protocol.DownSample;

/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 * 
 */
public class DownSampleUtilityTest {

    @Test
    public void testMinute() {
        Instant original = new Instant();
        Instant downSampled = DownSampleUtility.downSample(original, DownSample.MINUTE);
        assertEquals(0L, downSampled.get(millisOfSecond()));
        assertEquals(0L, downSampled.get(secondOfMinute()));
        assertEquals(original.get(minuteOfHour()), downSampled.get(minuteOfHour()));
        assertEquals(original.get(hourOfDay()), downSampled.get(hourOfDay()));
        assertEquals(original.get(dayOfMonth()), downSampled.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), downSampled.get(monthOfYear()));
        assertEquals(original.get(year()), downSampled.get(year()));
    }

    @Test
    public void testHour() {
        Instant original = new Instant();
        Instant downSampled = DownSampleUtility.downSample(original, DownSample.HOUR);
        assertEquals(0L, downSampled.get(millisOfSecond()));
        assertEquals(0L, downSampled.get(secondOfMinute()));
        assertEquals(0L, downSampled.get(minuteOfHour()));
        assertEquals(original.get(hourOfDay()), downSampled.get(hourOfDay()));
        assertEquals(original.get(dayOfMonth()), downSampled.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), downSampled.get(monthOfYear()));
        assertEquals(original.get(year()), downSampled.get(year()));
    }

    @Test
    public void testDay() {
        Instant original = new Instant();
        Instant downSampled = DownSampleUtility.downSample(original, DownSample.DAY);
        assertEquals(0L, downSampled.get(millisOfSecond()));
        assertEquals(0L, downSampled.get(secondOfMinute()));
        assertEquals(0L, downSampled.get(minuteOfHour()));
        assertEquals(0L, downSampled.get(hourOfDay()));
        assertEquals(original.get(dayOfMonth()), downSampled.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), downSampled.get(monthOfYear()));
        assertEquals(original.get(year()), downSampled.get(year()));

    }

    @Test
    public void testMonth() {
        Instant original = new Instant();
        Instant downSampled = DownSampleUtility.downSample(original, DownSample.MONTH);
        assertEquals(0L, downSampled.get(millisOfSecond()));
        assertEquals(0L, downSampled.get(secondOfMinute()));
        assertEquals(0L, downSampled.get(minuteOfHour()));
        assertEquals(0L, downSampled.get(hourOfDay()));
        assertEquals(1L, downSampled.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), downSampled.get(monthOfYear()));
        assertEquals(original.get(year()), downSampled.get(year()));
    }

    @Test
    public void testMonth_2() {
        Instant original = new Instant(new DateTime(1920, 2, 1, 3, 10, 31, 100));
        Instant downSampled = DownSampleUtility.downSample(original, DownSample.MONTH);
        assertEquals(0L, downSampled.get(millisOfSecond()));
        assertEquals(0L, downSampled.get(secondOfMinute()));
        assertEquals(0L, downSampled.get(minuteOfHour()));
        assertEquals(0L, downSampled.get(hourOfDay()));
        assertEquals(1L, downSampled.get(dayOfMonth()));
        assertEquals(original.get(monthOfYear()), downSampled.get(monthOfYear()));
        assertEquals(original.get(year()), downSampled.get(year()));
    }
}
