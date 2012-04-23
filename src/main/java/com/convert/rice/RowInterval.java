package com.convert.rice;

import static org.joda.time.DateTimeFieldType.millisOfDay;
import static org.joda.time.DateTimeFieldType.millisOfSecond;
import static org.joda.time.DateTimeFieldType.minuteOfHour;
import static org.joda.time.DateTimeFieldType.secondOfMinute;

import org.joda.time.Instant;

public enum RowInterval {
    MILLI,
    SECOND,
    MINUTE,
    HOUR,
    DAY;

    public int getMillis() {
        switch (this) {
        case MILLI:
            return 1;
        case SECOND:
            return 1000;
        case MINUTE:
            return 60000;
        case HOUR:
            return 3600000;
        case DAY:
            return 86400000;
        }
        throw new AssertionError("Unkown interval");
    }

    public long getStart(long timestamp) {
        Instant instant = new Instant(timestamp);
        switch (this) {
        case MILLI:
            return timestamp;
        case SECOND:
            return timestamp - instant.get(millisOfSecond());
        case MINUTE:
            return timestamp - instant.get(millisOfSecond()) - (1000 * instant.get(secondOfMinute()));

        case HOUR:
            return timestamp - instant.get(millisOfSecond())
                    - (1000 * instant.get(secondOfMinute()))
                    - (60000 * instant.get(minuteOfHour()));
        case DAY:
            return timestamp - instant.get(millisOfDay());
        }
        throw new AssertionError("Unknown interval");
    }

    static RowInterval fromString(String str) {
        if ("ms".equalsIgnoreCase(str)) {
            return MILLI;
        }

        if ("s".equalsIgnoreCase(str)) {
            return SECOND;
        }

        if ("m".equalsIgnoreCase(str)) {
            return MINUTE;
        }

        if ("h".equalsIgnoreCase(str)) {
            return HOUR;
        }
        if ("d".equalsIgnoreCase(str)) {
            return DAY;
        }
        throw new AssertionError("Unkown interval");
    }
}
