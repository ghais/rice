package com.convert.rice.functions.map;

import static java.util.Collections.singletonList;

import java.util.List;

import com.convert.rice.DataPoints;

public class Identity implements MapFunction {

    @Override
    public List<DataPoints> apply(DataPoints input) {
        return singletonList(input);
    }

}
