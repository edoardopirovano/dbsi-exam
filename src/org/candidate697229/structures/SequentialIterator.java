package org.candidate697229.structures;

public class SequentialIterator implements Iterator {
    private final long[][] tuples;
    private int position = 0;
    private int depth = 0;
    private boolean atEnd = false;

    public SequentialIterator(long[][] tuples) {
        this.tuples = tuples;
        for (int i = 0; i < tuples.length - 1; ++i) {
            if (tuples[i][0] > tuples[i + 1][0])
                throw new InternalError("Tuples are not sorted on the join attribute!");
        }
    }

    @Override
    public boolean atEnd() {
        return atEnd;
    }

    @Override
    public long key() {
        return tuples[position][depth];
    }

    @Override
    public boolean isNextKeySame() {
        return isNextInView() && tuples[position][depth] == tuples[position + 1][depth];
    }

    @Override
    public void seek(long x) {
        while (!atEnd && tuples[position][depth] != x)
            next();
    }

    private boolean isNextInView() {
        if (position == tuples.length - 1)
            return false;
        for (int i = 0; i < depth; ++i) {
            if (tuples[position][i] != tuples[position + 1][i])
                return false;
        }
        return true;
    }

    @Override
    public void next() {
        if (!isNextInView())
            atEnd = true;
        ++position;
    }

    @Override
    public void prev() {
        --position;
        atEnd = false;
    }

    @Override
    public long[] value() {
        return tuples[position];
    }

    public void up() {
        depth--;
        if (atEnd) {
            position--;
            if (isNextInView())
                atEnd = false;
        }
    }

    public void down() {
        depth++;
    }
}
