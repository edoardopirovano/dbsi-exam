package org.candidate697229.structures;

/**
 * Implementation of a data Iterator that iterates sequentially over the data.
 */
public class SequentialIterator implements Iterator {
    private final long[][] tuples;
    private int position = 0;
    private int depth = -1;
    private boolean atEnd = false;

    /**
     * Construct a new sequential iterator.
     *
     * @param tuples the tuples to iterate over
     */
    public SequentialIterator(long[][] tuples) {
        this.tuples = tuples;
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
    public void seek(long x) {
        while (!atEnd && tuples[position][depth] < x)
            next();
    }

    @Override
    public boolean isNextInBlock() {
        return isNextSameUpToDepth(depth + 1);
    }

    @Override
    public void nextInBlock() {
        position++;
    }

    @Override
    public void back(int numOfValues) {
        position -= numOfValues;
        atEnd = false;
    }

    @Override
    public void next() {
        long startValue = tuples[position][depth];
        while (isNextInView() && tuples[position][depth] == startValue)
            ++position;
        if (tuples[position][depth] == startValue)
            atEnd = true;
    }

    @Override
    public long[] value() {
        return tuples[position];
    }

    @Override
    public void open() {
        assert (!atEnd);
        depth++;
        while (isPreviousInView())
            --position;
    }

    @Override
    public void up() {
        depth--;
        atEnd = false;
    }

    /**
     * Check if the previous tuple is equal up to position depth.
     * Returns false if there is no previous tuple.
     * @return true if the current and previous tuple are equal up to position depth
     */
    private boolean isPreviousInView() {
        if (position == 0)
            return false;
        for (int i = 0; i < depth; ++i) {
            if (tuples[position][i] != tuples[position - 1][i])
                return false;
        }
        return true;
    }

    /**
     * Check if the next tuple is equal up to position depth.
     * Returns false if there is no next tuple.
     * @return true if the current and next tuple are equal up to position depth
     */
    private boolean isNextInView() {
        return isNextSameUpToDepth(depth);
    }

    /**
     * Check if the next tuple is equal up to position limitDepth.
     * Returns false if there is no next tuple.
     * @param limitDepth the position in the tuple to check up to
     * @return true if the current and next tuple are equal up to position depth
     */
    private boolean isNextSameUpToDepth(int limitDepth) {
        if (position == tuples.length - 1)
            return false;
        for (int i = 0; i < limitDepth; ++i) {
            if (tuples[position][i] != tuples[position + 1][i])
                return false;
        }
        return true;
    }
}
