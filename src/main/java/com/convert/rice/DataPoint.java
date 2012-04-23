package com.convert.rice;

public class DataPoint implements Comparable<DataPoint> {

    private final long value;

    private final long start;

    private final long end;

    public DataPoint(long start, long end, long value) {
        this.value = value;
        this.start = start;
        this.end = end;
    }

    /**
     * Returns the value of the this data point as a {@code long}.
     * 
     */
    public long getValue() {
        return this.value;
    }

    /**
     * <B>Deprecated.</B> User getStart().
     *
     * @return the timestamp
     */
    @Deprecated
    public long getTimestamp() {
        return start;
    }

    public long getStart() {
        return start;
    }

    /**
     * @return the end
     */
    public long getEnd() {
        return end;
    }

    @Override
    public int compareTo(DataPoint dp) {
        if (this.getStart() < dp.getStart()) {
            return -1;
        } else if (this.getStart() > dp.getStart()) {
            return 1;
        } else if (this.getValue() < dp.getValue()) {
            return -1;
        } else if (this.getValue() > dp.getValue()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (end ^ (end >>> 32));
        result = prime * result + (int) (start ^ (start >>> 32));
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
        if (end != other.end) {
            return false;
        }
        if (start != other.start) {
            return false;
        }
        if (value != other.value) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DataPoint [value=");
        builder.append(value);
        builder.append(", start=");
        builder.append(start);
        builder.append(", end=");
        builder.append(end);
        builder.append("]");
        return builder.toString();
    }

}
