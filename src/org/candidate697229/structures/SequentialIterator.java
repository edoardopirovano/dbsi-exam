package org.candidate697229.structures;

public class SequentialIterator implements Iterator {
    private final long[][] tuples;
    private int position = 0;

    public SequentialIterator(long[][] tuples) {
        this.tuples = tuples;
        for (int i = 0; i < tuples.length - 1; ++i) {
            if (tuples[i][0] > tuples[i + 1][0])
                throw new InternalError("Tuples are not sorted on the join attribute!");
        }
    }

    @Override
    public boolean atEnd() {
        return position == tuples.length;
    }

    @Override
    public long key() {
        return tuples[position][0];
    }

    @Override
    public boolean isNextKeySame() {
        return position < tuples.length - 2 && tuples[position][0] == tuples[position + 1][0];
    }

    @Override
    public void seek(long x) {
        while (!atEnd() && key() < x)
            next();
    }

    @Override
    public void next() {
        position++;
    }

    @Override
    public void prev() {
        position--;
    }

    @Override
    public long[] value() {
        return tuples[position];
    }
}
