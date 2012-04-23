package com.convert.rice.functions.reduce;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;

public class Max implements ReduceFunction {

    @Override
    public DataPoint apply(DataPoints input) {
        long max = 0;
        for (DataPoint dp : input) {
            if (dp.getValue() > max) {
                max = dp.getValue();
            }
        }
        return new DataPoint(input.getStart(), input.getEnd(), max);
    }

}
