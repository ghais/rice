package com.convert.rice.functions.reduce;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;

public class Median implements ReduceFunction {

    public Median() {
    }

    @Override
    public DataPoint apply(DataPoints input) {
        if (input.size() < 2) {
            return new DataPoint(input.getStart(), input.getEnd(), 0);
        }
        OnlineSummarizer summarizer = new OnlineSummarizer();
        for (DataPoint dp : input) {
            summarizer.add(dp.getValue());
        }
        return new DataPoint(input.getStart(), input.getEnd(), (long) summarizer.getMedian());
    }

}
