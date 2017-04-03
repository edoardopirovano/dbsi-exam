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
            }
        ).collect(Collectors.toList());
    }

    void init() {
        findAllBindings(false);
    }

    boolean atEnd() {
        return atEnd;
    }

    long[][] resultTuple() {
        for (int i = 0; i < k; ++i)
            resultTuple[i] = iterators.get(i).value();
        return resultTuple;
    }

    void next() {
        findAllBindings(true);
    }

    private void findAllBindings(boolean shouldDoNext) {
        int depth = unaryTrieJoins.size() - 1;
        do {
            while (depth >= 0) {
                if (!unaryTrieJoins.get(depth).atEnd() && shouldDoNext) {
                    unaryTrieJoins.get(depth).next();
                    shouldDoNext = false;
                }
                if (!unaryTrieJoins.get(depth).atEnd())
                    break;
                if (depth == 0)  {
                    atEnd = true;
                    return;
                }
                unaryTrieJoins.get(depth--).up();
            }
            depth++;
            while (depth < unaryTrieJoins.size()) {
                unaryTrieJoins.get(depth).down();
                if (unaryTrieJoins.get(depth).atEnd())
                    break;
                depth++;
            }
        } while (depth != unaryTrieJoins.size());
    }

}

class UnaryTrieJoin {
    private final List<Iterator> iterators;
    private int k;
    private long[] returnPositions;

    UnaryTrieJoin(List<Iterator> iterators) {
        this.iterators = iterators;
        k = iterators.size();
        returnPositions = new long[k];
    }

    private boolean atEnd = false;
    private int p = 0;

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

    boolean atEnd() {
        return atEnd;
    }

    void init() {
        atEnd = false;
        for (int i = 0; i < k; ++i) {
            if (iterators.get(i).atEnd())
                atEnd = true;
        }
        if (!atEnd)
            leapfrogSearch();
    }


    void down() {
        iterators.forEach(Iterator::down);
        leapfrogSearch();
    }

    void up() {
        iterators.forEach(Iterator::up);
    }
}