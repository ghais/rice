package com.convert.rice;

import java.util.NoSuchElementException;

public class DataPointsIterator implements SeekableView {

    private DataPoints dataPoints;

    private int index = 0;

    /**
     * @param defaultDataPoints
     */
    public DataPointsIterator(DataPoints dataPoints) {
        this.dataPoints = dataPoints;
    }

    @Override
    public boolean hasNext() {
        return index < dataPoints.size();
    }

    @Override
    public DataPoint next() {
        if (hasNext()) {
            return dataPoints.get(index++);
        }
        throw new NoSuchElementException("no more elements in " + this);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long timestamp) {
        // Do a binary search to find the timestamp given or the one right before.
        short lo = 0;
        short hi = (short) dataPoints.size();

        while (lo <= hi) {
            index = (short) ((lo + hi) >>> 1);
            long cmp = dataPoints.timestamp(index) - timestamp;

            if (cmp < 0) {
                lo = (short) (index + 1);
            } else if (cmp > 0) {
                hi = (short) (index - 1);
            } else {
                index--; // 'index' is exactly on the timestamp wanted.
                return; // So let's go right before that for next().
            }
        }
        // We found the timestamp right before as there was no exact match.
        // We take that position - 1 so the next call to next() returns it.
        index = (short) (lo - 1);
        // If the index we found was not the first or the last point, let's
        // do a small extra sanity check to ensure the position we found makes
        // sense: the timestamp we're at must not be >= what we're looking for.
        if (0 < index && index < dataPoints.size() && dataPoints.timestamp(index) >= timestamp) {
            throw new AssertionError("seeked after the time wanted!"
                    + " timestamp=" + timestamp
                    + ", index=" + index
                    + ", dp.timestamp(index)=" + dataPoints.timestamp(index)
                    + ", this=" + this);
        }

    }

    /**
     * Get the timestamp at the current index.
     * 
     * @return The value at the current index.
     * @throws NoSuchElementException
     *             if the are no more data points.
     */
    public long timestamp() {
        if (hasNext()) {
            return dataPoints.timestamp(index);
        }
        throw new NoSuchElementException("no more elements in " + this);
    }

    /**
     * Get the value at the current index.
     * 
     * @return The value at the current index.
     * @throws NoSuchElementException
     *             if there are no more data points.
     */
    public long value() {
        if (hasNext()) {
            return dataPoints.get(index).getValue();
        }
        throw new NoSuchElementException("no more elements in " + this);
    }

    @Override
    public DataPoint peek() {
        if (hasNext()) {
            return dataPoints.get(index);
        }
        throw new NoSuchElementException("no more elements in " + this);
    }

}
