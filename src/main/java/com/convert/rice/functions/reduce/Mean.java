package com.convert.rice.functions.reduce;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;

public class Mean implements ReduceFunction {

    public Mean() {
    }

    @Override
    public DataPoint apply(DataPoints input) {
        OnlineSummarizer summarizer = new OnlineSummarizer();
        for (DataPoint dp : input) {
            summarizer.add(dp.getValue());
        }
        return new DataPoint(input.getStart(), input.getEnd(), (long) summarizer.getMean());
    }

}
