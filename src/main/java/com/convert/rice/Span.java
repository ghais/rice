package com.convert.rice;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class Span extends TreeSet<WritableDataPoints> {

    PeekingIterator<WritableDataPoints> _thisIterator;

    public Span(SortedSet<WritableDataPoints> dps) {
        super(dps);
    }

    public Span() {
        super(new Comparator<DataPoints>() {

            @Override
            public int compare(DataPoints o1, DataPoints o2) {
                if (o1.getStart() < o2.getStart()) {
                    return -1;
                }

                if (o1.getStart() > o2.getStart()) {
                    return 1;
                }

                if (o1.getStart() == o2.getStart()) {
                    if (o1.getEnd() < o2.getEnd()) {
                        return -1;
                    }
                    if (o1.getEnd() > o2.getEnd()) {
                        return 1;
                    }
                }
                return 0;
            }
        });
    }

    public boolean add(DataPoint dp) {
        if (null == _thisIterator) {
            _thisIterator = Iterators.peekingIterator(this.iterator());
            if (!_thisIterator.hasNext()) {
                return false;
            }
        }
        // Can we insert it at the current position?
        if (_thisIterator.peek().getStart() <= dp.getStart() && _thisIterator.peek().getEnd() > dp.getStart()) {
            _thisIterator.peek().add(dp);
            return true;
        }
        // Check if the point fits somewhere in the future.
        while (_thisIterator.hasNext()) {
            WritableDataPoints wdps = _thisIterator.peek();
            if (wdps.getStart() <= dp.getStart() && wdps.getEnd() > dp.getStart()) {
                wdps.add(dp);
                return true;
            }
            _thisIterator.next();
        }

        this._thisIterator = null; // we have exhausted the iterator.

        // At this point try inserting from the start.
        for (WritableDataPoints wdps : this) {
            if (wdps.getStart() <= dp.getStart() && wdps.getEnd() > dp.getStart()) {
                wdps.add(dp);
                return true;
            }
        }
        // At this point we can't add the data point.
        return false;
    }

}
