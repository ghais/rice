package com.convert.rice;

import java.io.IOException;
import java.util.Map;

import org.joda.time.Interval;

import com.convert.rice.protocol.DownSample;

public interface TimeSeries {

    Map<String, DataPoints> get(String type, String key, Interval interval) throws IOException;

    void inc(String type, String key, long timestamp, Map<String, Long> dps, DownSample downSample) throws IOException;
}
