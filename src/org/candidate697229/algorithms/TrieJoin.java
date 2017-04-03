package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.structures.Iterator;
import org.candidate697229.structures.SequentialIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class TrieJoin {
    private final Iterator[] iterators;
    private final UnaryTrieJoin[] unaryTrieJoins;
    private int k;
    private long[][] resultTuple;
    private boolean overallAtEnd = false;
    private int depth = -1;

    TrieJoin(Database database, List<List<int[]>> joinInstructions) {
        k = database.getRelations().size();
        iterators = new Iterator[k];
        for (int i = 0; i < k; ++i)
            iterators[i] = new SequentialIterator(database.getRelations().get(i).getTuples());
        resultTuple = new long[k][];
        unaryTrieJoins = joinInstructions.stream().map(joinInstruction -> {
                List<Iterator> usedIterators = new ArrayList<>(joinInstruction.size());
                usedIterators.addAll(joinInstruction.stream().map(position -> iterators[position[0]]).collect(Collectors.toList()));
                return new UnaryTrieJoin(usedIterators);
            }).collect(Collectors.toList()).toArray(new UnaryTrieJoin[0]);
    }

    long[][] resultTuple() {
        for (int i = 0; i < k; ++i)
            resultTuple[i] = iterators[i].value();
        return resultTuple;
    }

    void init() {
        findNext(false);
    }

    boolean overallAtEnd() {
        return overallAtEnd;
    }

    void overallNext() { findNext(true);}

    private void findNext(boolean shouldAdvance) {
        do {
            while (depth > 0 && atEnd()) {
                up();
                next();
                if (!atEnd())
                    shouldAdvance = false;
            }
            if (depth == 0 && atEnd()) {
                overallAtEnd = true;
                return;
            }
            if (shouldAdvance) {
                next();
                shouldAdvance = atEnd();
            }
            while (depth < unaryTrieJoins.length - 1) {
                open();
                if (atEnd())
                    break;
            }
        } while (atEnd());
    }

    private boolean atEnd() {
        return unaryTrieJoins[depth].atEnd();
    }

    private void next() {
        unaryTrieJoins[depth].next();
    }

    private void open() {
        unaryTrieJoins[++depth].open();
    }

    private void up() {
        unaryTrieJoins[depth--].up();
    }
}

class UnaryTrieJoin {
    private final Iterator[] iterators;
    private int k;

    UnaryTrieJoin(List<Iterator> iterators) {
        this.iterators = iterators.toArray(new Iterator[0]);
        k = iterators.size();
    }

    private boolean atEnd;
    private int p = 0;

    private void init() {
        atEnd = false;
        for (int i = 0; i < k; ++i) {
            if (iterators[i].atEnd())
                atEnd = true;
        }
        if (!atEnd)
            leapfrogSearch();
    }

    private void leapfrogSearch() {
        long x1 = iterators[Math.floorMod(p - 1, k)].key();
        while (true) {
            long x = iterators[p].key();
            if (x == x1) {
                return;
            } else {
                iterators[p].seek(x1);
                if (iterators[p].atEnd()) {
                    atEnd = true;
                    return;
                } else {
                    x1 = iterators[p].key();
                    p = Math.floorMod(p + 1, k);
                }
            }
        }
    }

    void next() {
        iterators[p].next();
        if (iterators[p].atEnd())
            atEnd = true;
        else {
            p = Math.floorMod(p + 1, k);
            leapfrogSearch();
        }
    }

    boolean atEnd() {
        return atEnd;
    }

    void open() {
        for (Iterator iterator : iterators) iterator.open();
        init();
    }

    void up() {
        for (Iterator iterator : iterators) iterator.up();
    }
}