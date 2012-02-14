package com.convert.rice;

public class DataPoint {

    private final long value;

    private final long timestamp;

    public DataPoint(long value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    /**
     * Returns the value of the this data point as a {@code long}.
     * 
     */
    public long getValue() {
        return this.value;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        result = prime * result + (int) (value ^ (value >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DataPoint other = (DataPoint) obj;
        if (timestamp != other.timestamp) {
            return false;
        }
        if (value != other.value) {
            return false;
        }
        return true;
    }

}
