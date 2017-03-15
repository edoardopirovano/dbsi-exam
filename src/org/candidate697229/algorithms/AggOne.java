package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Table;
import org.candidate697229.structures.BTree;
import org.candidate697229.util.ImmutablePair;

import java.util.ArrayList;
import java.util.List;

public class AggOne {
    private final List<int[]> distinctAttributes;
    private final TrieJoin trieJoin;
    private long[] result;

    public AggOne(Database database) {
        ImmutablePair<List<int[]>, List<int[]>> conditionsAndDistinct = database.getConditionsAndDistinct();
        trieJoin = new TrieJoin(database, conditionsAndDistinct.getFirst());
        trieJoin.init();
        distinctAttributes = conditionsAndDistinct.getSecond();

    }

    public long[] computeAllAggregatesOfNaturalJoin() {
        int totalAggregates = (distinctAttributes.size() * (distinctAttributes.size() + 1)) / 2;
        result = new long[totalAggregates];

        while (!trieJoin.atEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            int agg = 0;
            for (int j = 0; j < distinctAttributes.size(); ++j) {
                int[] firstAttribute = distinctAttributes.get(j);
                for (int k = j; k < distinctAttributes.size(); ++k) {
                    int[] secondAttribute = distinctAttributes.get(k);
                    result[agg] += tuple[firstAttribute[0]][firstAttribute[1]]
                            * tuple[secondAttribute[0]][secondAttribute[1]];
                    ++agg;
                }
            }
            trieJoin.next();
        }

        return result;
    }

    public long computeOneAggregateOfNaturalJoin() {
        long result = 0;

        while (!trieJoin.atEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            result += tuple[0][1] * tuple[1][1];
            trieJoin.next();
        }

        return result;
    }
}

class TrieJoin {
    private ArrayList<BTree<Long, long[]>> iterators;
    private int k;
    private long[][] resultTuple;
    private long[] returnPositions;

    TrieJoin(Database database, List<int[]> joinCondition) {
        k = database.getTables().size();
        iterators = new ArrayList<>(k);
        for (Table table : database.getTables()) {
            BTree<Long, long[]> tree = new BTree<>();
            for (long[] tuple : table.getTuples())
                tree.put(tuple[0], tuple);
            tree.clearIterator();
            iterators.add(tree);
        }
        resultTuple = new long[k][];
        returnPositions = new long[k];

        /*
         Verify that our simplifying assumption that we are joining precisely on the first fields
         of each table does indeed hold.
          */
        assert (joinCondition.size() == k - 1);
        for (int i = 0; i < k - 1; ++i)
            assert (joinCondition.get(i)[0] == 0 && joinCondition.get(i)[1] == 0
                    && joinCondition.get(i)[2] == i + 1 && joinCondition.get(i)[3] == 0);
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