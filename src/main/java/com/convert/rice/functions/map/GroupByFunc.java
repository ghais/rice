package com.convert.rice.functions.map;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.Span;
import com.convert.rice.WritableDataPoints;
import com.convert.rice.protocol.MapReduce.GroupBy;
import com.convert.rice.protocol.MapReduce.Interval;

public class GroupByFunc implements MapFunction {

    private final GroupBy groupBy;

    public GroupByFunc(GroupBy groupBy) {
        this.groupBy = checkNotNull(groupBy);
    }

    @Override
    public List<DataPoints> apply(DataPoints input) {
        Span result = groupBy.hasStep() ? getBrackets(input, groupBy.getStep()) : getBrackets(
                input, groupBy.getIntervalsList());
        for (DataPoint dp : input) {
            result.add(dp);
        }
        return new ArrayList<DataPoints>(result);
    }

    /**
     * @param input
     * @param intervalsList
     * @return
     */
    private Span getBrackets(DataPoints input, List<Interval> intervalsList) {
        Span span = new Span();
        for (Interval interval : intervalsList) {
            span.add(
                    new WritableDataPoints(input.getKey(), input.getMetricName(), interval.getStart(), interval
                            .getEnd()));
        }

        return span;
    }

    private Span getBrackets(DataPoints input, long step) {
        Span span = new Span();

        int brackets = (int) Math.ceil((double) (input.getEnd() - input.getStart()) / step);
        for (int i = 0; i < brackets; i++) {
            long start = input.getStart() + (step * i);
            long end = input.getStart() + (step * (i + 1));
            span.add(new WritableDataPoints(input.getKey(), input.getMetricName(), start, end));
        }
        return span;
    }

}
