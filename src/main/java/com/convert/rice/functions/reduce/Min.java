package com.convert.rice.functions.reduce;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;

public class Min implements ReduceFunction {

    @Override
    public DataPoint apply(DataPoints input) {
        if (input.size() == 0) {
            return new DataPoint(input.getStart(), input.getEnd(), 0);
        }
        long min = Long.MAX_VALUE;
        for (DataPoint dp : input) {
            if (dp.getValue() < min) {
                min = dp.getValue();
            }
        }
        return new DataPoint(input.getStart(), input.getEnd(), min);
    }

}
