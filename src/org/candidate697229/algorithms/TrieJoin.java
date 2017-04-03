package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Table;
import org.candidate697229.structures.BTreeIterator;
import org.candidate697229.structures.Iterator;
import org.candidate697229.structures.SequentialIterator;

import java.util.ArrayList;

class TrieJoin {
    private static final boolean USE_B_TREE = false;

    private ArrayList<Iterator> iterators;
    private int k;
    private long[][] resultTuple;
    private long[] returnPositions;

    TrieJoin(Database database) {
        k = database.getTables().size();
        iterators = new ArrayList<>(k);
        for (Table table : database.getTables()) {
            if (USE_B_TREE) iterators.add(new BTreeIterator(table.getTuples()));
            else iterators.add(new SequentialIterator(table.getTuples()));
        }
        resultTuple = new long[k][];
        returnPositions = new long[k];
    }

    private boolean atEnd = false;
    private int p = 0;

    void init() {
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
                // Set p to first iterator with duplicates, if applicable
                for (int i = 0; i < k; ++i) {
                    if (iterators.get(i).isNextKeySame()) {
                        p = i;
                        return;
                    }
                }
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
        long previousKey = iterators.get(p).key();
        iterators.get(p).next();
        if (iterators.get(p).atEnd() && returnPositions[p] == 0) {
            atEnd = true;
            return;
        }
        if (!iterators.get(p).atEnd() && iterators.get(p).key() == previousKey) {
            returnPositions[p] += 1;
            return;
        }
        if (returnPositions[p] == 0) {
            p = Math.floorMod(p + 1, k);
            leapfrogSearch();
        } else {
            for (int i = 0; i <= returnPositions[p]; ++i)
                iterators.get(p).prev();
            returnPositions[p] = 0;

            for (int i = 1; i < k; ++i) {
                int j = Math.floorMod(p + i, k);
                iterators.get(j).next();
                if (!iterators.get(j).atEnd() && iterators.get(j).key() == previousKey) {
                    returnPositions[j] += 1;
                    return;
                }
                if (returnPositions[j] > 0) {
                    for (int l = 0; l <= returnPositions[j]; ++l)
                        iterators.get(j).prev();
                    returnPositions[j] = 0;
                } else iterators.get(j).prev();
            }

            // If we get to here we have iterated all duplicates
            while (!iterators.get(p).atEnd() && iterators.get(p).key() == previousKey)
                iterators.get(p).next();
            if (iterators.get(p).atEnd()) {
                atEnd = true;
                return;
            }
            p = Math.floorMod(p + 1, k);
            leapfrogSearch();
        }
    }

    long[][] resultTuple() {
        for (int i = 0; i < k; ++i)
            resultTuple[i] = iterators.get(i).value();
        return resultTuple;
    }

    boolean atEnd() {
        return atEnd;
    }
}
