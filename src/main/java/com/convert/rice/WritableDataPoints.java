package com.convert.rice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;

public class WritableDataPoints implements DataPoints {

    private final String key;

    private final String metricName;

    private final List<DataPoint> dataPoints;

    private final long start;

    private final long end;

    public WritableDataPoints(String key, String metricName, Iterable<DataPoint> dps, long start, long end) {
        this.key = checkNotNull(key, "key");
        this.metricName = checkNotNull(metricName, "metric name");
        this.dataPoints = newArrayList(checkNotNull(dps, "data points"));
        this.start = start;
        this.end = end;
    }

    /**
     * 
     * @param key
     * @param metricName
     */
    public WritableDataPoints(String key, String metricName, long start, long end) {
        this(key, metricName, new ArrayList<DataPoint>((int) ((end - start) / end)), start, end);
    }

    @Override
    public String getMetricName() {
        return this.metricName;
    }

    @Override
    public long timestamp(int i) {
        return dataPoints.get(i).getStart();
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
        checkArgument(
                dp.getStart() >= this.start,
                String.format("Datapoint should be after the start of the interval",
                        this.start, this.end, dp.getStart(), dp.getEnd()));
        checkArgument(dp.getStart() < this.end, String.format(
                "Datapoint should be before the end of the interval this[%d,%d) , dp[%d,%d)",
                this.start, this.end, dp.getStart(), dp.getEnd()));
        checkArgument(dp.getEnd() <= this.end, String.format("Datapoint should be before the end of the interval",
                this.start, this.end, dp.getStart(), dp.getEnd()));

        if (this.dataPoints.isEmpty()) {
            this.dataPoints.add(dp);
            return;
        }

        checkArgument(dp.getStart() >= this.dataPoints.get(this.dataPoints.size() - 1).getStart());
        this.dataPoints.add(dp);
    }

    /**
     * @return the key
     */
    @Override
    public String getKey() {
        return key;
    }

    @Override
    public long getStart() {
        return this.start;
    }

    @Override
    public long getEnd() {
        return this.end;
    }

    @Override
    public DataPoints mapReduce(Function<DataPoints, List<DataPoints>> map, Function<DataPoints, DataPoint> reduce) {
        List<DataPoints> spans = map.apply(this);
        List<DataPoint> transform = transform(spans, reduce);
        return new WritableDataPoints(key, metricName, transform, start, end);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("WritableDataPoints [key=");
        builder.append(key);
        builder.append(", metricName=");
        builder.append(metricName);
        builder.append(", dataPoints=");
        builder.append(dataPoints);
        builder.append(", start=");
        builder.append(start);
        builder.append(", end=");
        builder.append(end);
        builder.append("]");
        return builder.toString();
    }

}
