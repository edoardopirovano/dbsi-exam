package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Table;
import org.candidate697229.structures.Iterator;
import org.candidate697229.structures.SequentialIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class TrieJoin {
    //private static final boolean USE_B_TREE = false;

    private final List<Iterator> iterators;
    private final List<UnaryTrieJoin> unaryTrieJoins;
    private int k;
    private long[][] resultTuple;
    private boolean atEnd = false;
    private int depth = -1;

    TrieJoin(Database database, List<List<int[]>> joinInstructions) {
        k = database.getTables().size();
        iterators = new ArrayList<>(k);
        for (Table table : database.getTables()) {
            //if (USE_B_TREE) iterators.add(new BTreeIterator(table.getTuples()));
            iterators.add(new SequentialIterator(table.getTuples()));
        }
        resultTuple = new long[k][];
        unaryTrieJoins = joinInstructions.stream().map(joinInstruction -> {
                List<Iterator> usedIterators = new ArrayList<>(joinInstruction.size());
                usedIterators.addAll(joinInstruction.stream().map(position -> iterators.get(position[0])).collect(Collectors.toList()));
                return new UnaryTrieJoin(usedIterators);
            }).collect(Collectors.toList());
    }

    long[][] resultTuple() {
        for (int i = 0; i < k; ++i)
            resultTuple[i] = iterators.get(i).value();
        return resultTuple;
    }

    private boolean atEnd() {
        return unaryTrieJoins.get(depth).atEnd();
    }

    private void next() {
        unaryTrieJoins.get(depth).next();
    }

    void init() {
        findNext(false);
    }

    boolean overallAtEnd() {
        return atEnd;
    }

    void overallNext() {
        findNext(true);
    }

    private void findNext(boolean shouldAdvance) {
        do {
            while (depth > 0 && atEnd()) {
                up();
                next();
                if (!atEnd())
                    shouldAdvance = false;
            }
            if (depth == 0 && atEnd()) {
                atEnd = true;
                return;
            }
            if (shouldAdvance) {
                next();
                shouldAdvance = atEnd();
            }
            while (depth < unaryTrieJoins.size() - 1) {
                open();
                if (atEnd())
                    break;
            }
        } while (atEnd());
    }

    private void open() {
        unaryTrieJoins.get(++depth).open();
    }

    private void up() {
        unaryTrieJoins.get(depth--).up();
    }
}

class UnaryTrieJoin {
    private final List<Iterator> iterators;
    private int k;

    UnaryTrieJoin(List<Iterator> iterators) {
        this.iterators = iterators;
        k = iterators.size();
    }

    private boolean atEnd;
    private int p = 0;

    private void init() {
        atEnd = false;
        for (int i = 0; i < k; ++i) {
            if (iterators.get(i).atEnd())
                atEnd = true;
        }
        if (!atEnd)
            leapfrogSearch();
    }

    private void leapfrogSearch() {
        long x1 = iterators.get(Math.floorMod(p - 1, k)).key();
        while (true) {
            long x = iterators.get(p).key();
            if (x == x1) {
                return;
            } else {
                iterators.get(p).seek(x1);
                if (iterators.get(p).atEnd()) {
                    atEnd = true;
                    return;
                } else {
                    x1 = iterators.get(p).key();
                    p = Math.floorMod(p + 1, k);
                }
            }
        }
    }

    void next() {
        iterators.get(p).next();
        if (iterators.get(p).atEnd())
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
        iterators.forEach(Iterator::open);
        init();
    }

    void up() {
        iterators.forEach(Iterator::up);
    }
}