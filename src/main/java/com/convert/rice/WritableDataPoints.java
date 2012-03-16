package com.convert.rice;

import static com.convert.rice.AggregationUtility.newMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.convert.rice.protocol.Aggregation;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class WritableDataPoints implements DataPoints {

    private final static Logger LOGGER = Logger.getLogger(WritableDataPoints.class.getName());

    private final String key;

    private final String metricName;

    private final List<DataPoint> dataPoints;

    private final long start;

    private final long end;

    public WritableDataPoints(String key, String metricName, List<DataPoint> dps, long start, long end) {
        this.key = checkNotNull(key, "key");
        this.metricName = checkNotNull(metricName, "metric name");
        this.dataPoints = checkNotNull(dps, "data points");
        this.start = start;
        this.end = end;
    }

    /**
     * 
     * @param key
     * @param metricName
     */
    public WritableDataPoints(String key, String metricName, long start, long end) {
        this(key, metricName, new ArrayList<DataPoint>(), start, end);
    }

    @Override
    public String getMetricName() {
        return this.metricName;
    }

    @Override
    public long timestamp(int i) {
        return dataPoints.get(i).getTimestamp();
    }

    @Override
    public SeekableView iterator() {
        return new DataPointsIterator(this);
    }

    @Override
    public int size() {
        return dataPoints.size();
    }

    @Override
    public DataPoint get(int i) {
        return dataPoints.get(i);
    }

    /**
     * add a datapoint to the datapoints.
     * 
     * @param dp
     * @throws IllegalArgumentException
     *             if the data point's timestamp is less than the last datapoint.
     * @throws IllegalArgumentException
     *             if the data point's time stamp is out side the [start,end) interval.
     */
    public void add(DataPoint dp) {
        checkArgument(dp.getTimestamp() >= this.start, "Datapoint should be after the start of the interval");
        checkArgument(dp.getTimestamp() < this.end, "Datapoint should be before the end of the interval");

        if (this.dataPoints.isEmpty()) {
            this.dataPoints.add(dp);
            return;
        }

        checkArgument(dp.getTimestamp() >= this.dataPoints.get(this.dataPoints.size() - 1).getTimestamp());
        this.dataPoints.add(dp);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DefaultDataPoints [metricName=");
        builder.append(metricName);
        builder.append(", dataPoints=");
        builder.append(dataPoints);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public String toJson() {
        Iterable<String> elements = Iterables.transform(this.dataPoints, new Function<DataPoint, String>() {

            @Override
            public String apply(DataPoint dp) {
                StringBuilder builder = new StringBuilder(29); // size is 13 * 2 + 3
                builder.append('[');
                builder.append(dp.getTimestamp());
                builder.append(',');
                builder.append(dp.getValue());
                builder.append(']');
                return builder.toString();
            }
        });
        String result = '[' + StringUtils.join(elements.iterator(), ',') + ']';
        return result;
    }

    /**
     * @return the key
     */
    @Override
    public String getKey() {
        return key;
    }

    @Override
    public SortedMap<Long, Long> aggregate(Aggregation aggregation) {
        SortedMap<Long, Long> values = newMap(this.getStart(), this.getEnd(), aggregation);
        for (DataPoint dp : this) {
            long ts = AggregationUtility.aggregateTo(dp.getTimestamp(), aggregation).getMillis();
            long v = dp.getValue();
            if (values.containsKey(ts)) {
                values.put(ts, values.get(ts) + v);
            } else {
                LOGGER.severe("The Aggregation utility should have created a map with this timestamp: " + ts + "["
                        + new DateTime(ts) + "]");
                values.put(ts, v);
            }
        }
        return values;
    }

    @Override
    public long getStart() {
        return this.start;
    }

    @Override
    public long getEnd() {
        return this.end;
    }
}
