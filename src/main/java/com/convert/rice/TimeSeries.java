package com.convert.rice;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.joda.time.Interval;

public interface TimeSeries {

    Collection<DataPoints> get(String type, String key, Interval interval, Collection<String> metrics)
            throws IOException;

    void inc(String type, String key, long timestamp, Map<String, Long> dps) throws IOException;
}
