package com.convert.rice;

import java.util.Iterator;

public interface SeekableView extends Iterator<DataPoint> {

    void seek(long timestamp);

    DataPoint peek();
}
