/**
 * (C) 2011 Digi-Net Technologies, Inc.
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA
 * All rights reserved.
 */
package com.convert.rice;

import java.util.SortedMap;

import com.convert.rice.protocol.Aggregation;

public interface DataPoints extends Iterable<DataPoint> {

    /**
     * Returns the name of the series.
     * 
     * @return the metric name.
     */
    String getMetricName();

    /**
     * Returns the metric key.
     * 
     * @return the metric key.
     */
    String getKey();

    /**
     * Return the number of points in this metric.
     * 
     * @return
     */
    int size();

    /**
     * Returns the timestamp associated with the {@code i}th data point. The first data point has index 0.
     * <p>
     * This method must be implemented in <code>O({@link #aggregatedSize})</code> or better.
     * <p>
     * It is guaranteed that
     * 
     * <pre>
     * timestamp(i) &lt; timestamp(i + 1)
     * </pre>
     * 
     * @param i
     *            index for the {@code i}th data point.
     * @return A strictly positive integer.
     * @throws IndexOutOfBoundsException
     *             if {@code i} is not in the range <code>[0, {@link #size} - 1]</code>
     */
    long timestamp(int i);

    /**
     * @param i
     *            an index for the {@code i}th element.
     * @return The value of the {@code i}th data point as a long. The first data point has index 0.
     * 
     * @throws IndexOutOfBoundsException
     *             if {@code i} is not in the range <code>[0, {@link #size} - 1]</code>
     */
    DataPoint get(int i);

    @Override
    SeekableView iterator();

    /**
     * @return
     */
    String toJson();

    /**
     * Get the view as a sorted map based on the given aggregation.
     * 
     * @return map from timestamp to values aggregated according to the given aggregator.
     */
    SortedMap<Long, Long> aggregate(Aggregation agg);

}
