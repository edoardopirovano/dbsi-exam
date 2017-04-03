package org.candidate697229.algorithms;

import org.candidate697229.database.Database;

public class AggOne {
    private final int[][] distinctPairs;
    private final TrieJoin trieJoin;
    private long[] result;

    public AggOne(Database database) {
        distinctPairs = database.getAllPairsOfColums().toArray(new int[0][]);
        trieJoin = new TrieJoin(database, database.getAllExplicitJoinConditions());
        trieJoin.init();
    }

    public long[] computeAllAggregatesOfNaturalJoin() {
        result = new long[distinctPairs.length];
        while (!trieJoin.overallAtEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            int agg = 0;
            for (int[] distinctPair : distinctPairs)
                result[agg++] += calculateFromInstruction(distinctPair, tuple);
            trieJoin.overallNext();
        }
        return result;
    }

    private long calculateFromInstruction(int[] instruction, long[][] tuple) {
        return tuple[instruction[0]][instruction[1]] * tuple[instruction[2]][instruction[3]];
    }

    public long computeOneAggregateOfNaturalJoin() {
        long result = 0;

        while (!trieJoin.overallAtEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            result += calculateFromInstruction(distinctPairs[0], tuple);
            trieJoin.overallNext();
        }

        return result;
    }
}

