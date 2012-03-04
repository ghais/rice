/**
 * (C) 2011 Digi-Net Technologies, Inc.
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA
 * All rights reserved.
 */
package com.convert.rice;

import static org.joda.time.DateTimeFieldType.dayOfMonth;
import static org.joda.time.DateTimeFieldType.millisOfSecond;
import static org.joda.time.DateTimeFieldType.minuteOfHour;
import static org.joda.time.DateTimeFieldType.secondOfDay;
import static org.joda.time.DateTimeFieldType.secondOfMinute;

import org.joda.time.Instant;

import com.convert.rice.protocol.DownSample;

/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 * 
 */
public class DownSampleUtility {

    static long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;

    static long MILLIS_IN_MINUTE = 60 * 1000;

    public static Instant downSample(Instant instant, DownSample downSample) {
        switch (downSample) {
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
}
