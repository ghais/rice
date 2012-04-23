package com.convert.rice.functions.reduce;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;

public class Sum implements ReduceFunction {

    @Override
    public DataPoint apply(DataPoints input) {
        long sum = 0L;
        for (DataPoint dp : input) {
            sum += dp.getValue();
        }
        return new DataPoint(input.getStart(), input.getEnd(), sum);
    }

}
