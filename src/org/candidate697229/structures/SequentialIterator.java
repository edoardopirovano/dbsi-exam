package org.candidate697229.structures;

import java.util.Stack;

public class SequentialIterator implements Iterator {
    private final long[][] tuples;
    private int position = 0;
    private int depth = -1;
    private boolean atEnd = false;
    private Stack<Integer> returnPositions = new Stack<>();

    public SequentialIterator(long[][] tuples) {
        this.tuples = tuples;
        // TODO check sorted
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

    public void open() {
        depth++;
        returnPositions.push(position);
    }

    public void up() {
        depth--;
        position = returnPositions.pop();
        atEnd = false;
    }
}
