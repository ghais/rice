package com.convert.rice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

import com.convert.rice.protocol.Aggregation;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class WritableDataPoints implements DataPoints {

    private final String key;

    private final String metricName;

    private final List<DataPoint> dataPoints;

    /**
     * Constructor.
     * 
     * @param key
     * @param metricName
     * @param dps
     */
    public WritableDataPoints(String key, String metricName, List<DataPoint> dps) {
        this.key = checkNotNull(key, "key");
        this.metricName = checkNotNull(metricName, "metric name");
        this.dataPoints = checkNotNull(dps, "data points");
    }

    /**
     * 
     * @param key
     * @param metricName
     */
    public WritableDataPoints(String key, String metricName) {
        this(key, metricName, new ArrayList<DataPoint>());
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
     */
    public void add(DataPoint dp) {
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
        SortedMap<Long, Long> values = new TreeMap<Long, Long>();
        for (DataPoint dp : this) {
            Long ts = AggregationUtility.aggregateTo(dp.getTimestamp(), aggregation).getMillis();
            Long v = dp.getValue();
            if (values.containsKey(ts)) {
                values.put(ts, values.get(ts) + v);
            } else {
                values.put(ts, v);
            }
        }
        return values;
    }
}
