/**
 * (C) 2011 Digi-Net Technologies, Inc.
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA
 * All rights reserved.
 */
package com.convert.rice;

import static com.google.common.base.Preconditions.checkArgument;
import static org.joda.time.DateTimeFieldType.dayOfMonth;
import static org.joda.time.DateTimeFieldType.millisOfSecond;
import static org.joda.time.DateTimeFieldType.minuteOfHour;
import static org.joda.time.DateTimeFieldType.secondOfDay;
import static org.joda.time.DateTimeFieldType.secondOfMinute;

import java.util.SortedMap;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

import com.convert.rice.protocol.Aggregation;

/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 * 
 */
public class AggregationUtility {

    static long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;

    static long MILLIS_IN_MINUTE = 60 * 1000;

    public static Instant aggregateTo(long timestamp, Aggregation aggregation) {
        return aggregateTo(new Instant(timestamp), aggregation);
    }

    public static Instant aggregateTo(Instant instant, Aggregation aggregation) {
        switch (aggregation) {
        case SECOND:
            return new Instant(instant.getMillis() - instant.get(millisOfSecond()));
        case MINUTE:
            return new Instant(instant.getMillis() - instant.get(millisOfSecond())
                    - (1000 * instant.get(secondOfMinute())));

        case HOUR:
            return new Instant(instant.getMillis() - instant.get(millisOfSecond())
                    - (1000 * instant.get(secondOfMinute()))
                    - (MILLIS_IN_MINUTE * instant.get(minuteOfHour())));
        case DAY:
            return new Instant(instant.getMillis() - instant.get(millisOfSecond())
                    - (1000 * instant.get(secondOfDay())));
        case MONTH:
            return new Instant(instant.getMillis() - instant.get(millisOfSecond())
                    - (1000 * instant.get(secondOfDay()))
                    - (MILLIS_IN_DAY * (instant.get(dayOfMonth()) - 1)));
        default:
            throw new IllegalStateException();
        }
    }

    public static SortedMap<Long, Long> newMap(long start, long end, Aggregation aggregation) {
        checkArgument(start <= end);
        switch (aggregation) {
        case SECOND:
            return newSecondsMap(start, end);
        case MINUTE:
            return newMinutesMap(start, end);
        case HOUR:
            return newHoursMap(start, end);
        case DAY:
            return newDaysMap(start, end);
        case MONTH:
            return newMonthsMap(start, end);
        }
        throw new IllegalStateException("Unkown Aggregation");
    }

    /**
     * @param start
     * @param end
     * @return
     */
    private static SortedMap<Long, Long> newSecondsMap(long start, long end) {
        SortedMap<Long, Long> result = new TreeMap<Long, Long>();
        DateTime current = new DateTime(aggregateTo(start, Aggregation.SECOND), DateTimeZone.UTC);
        while (current.getMillis() < end) {
            result.put(current.getMillis(), 0L);
            current = current.plusSeconds(1);
        }
        return result;
    }

    private static SortedMap<Long, Long> newMinutesMap(long start, long end) {
        SortedMap<Long, Long> result = new TreeMap<Long, Long>();
        DateTime current = new DateTime(aggregateTo(start, Aggregation.MINUTE), DateTimeZone.UTC);
        while (current.getMillis() < end) {
            result.put(current.getMillis(), 0L);
            current = current.plusMinutes(1);
        }
        return result;
    }

    private static SortedMap<Long, Long> newHoursMap(long start, long end) {
        SortedMap<Long, Long> result = new TreeMap<Long, Long>();
        DateTime current = new DateTime(aggregateTo(start, Aggregation.HOUR), DateTimeZone.UTC);
        while (current.getMillis() < end) {
            result.put(current.getMillis(), 0L);
            current = current.plusHours(1);
        }
        return result;
    }

    private static SortedMap<Long, Long> newDaysMap(long start, long end) {
        SortedMap<Long, Long> result = new TreeMap<Long, Long>();
        DateTime current = new DateTime(aggregateTo(start, Aggregation.DAY), DateTimeZone.UTC);
        while (current.getMillis() < end) {
            result.put(current.getMillis(), 0L);
            current = current.plusDays(1);
        }
        return result;
    }

    private static SortedMap<Long, Long> newMonthsMap(long start, long end) {
        SortedMap<Long, Long> result = new TreeMap<Long, Long>();
        DateTime current = new DateTime(aggregateTo(start, Aggregation.MONTH), DateTimeZone.UTC);
        while (current.getMillis() < end) {
            result.put(current.getMillis(), 0L);
            current = current.plusMonths(1);
        }
        return result;
    }

}
