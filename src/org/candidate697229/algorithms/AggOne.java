package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.util.ImmutablePair;

import java.util.List;

public class AggOne {
    private final List<int[]> distinctPairs;
    private final TrieJoin trieJoin;
    private long[] result;

    public AggOne(Database database) {
        ImmutablePair<List<int[]>, List<int[]>> conditionsAndDistinct = database.getConditionsAndDistinct();
        trieJoin = new TrieJoin(database, conditionsAndDistinct.getFirst());
        trieJoin.init();
        distinctPairs = conditionsAndDistinct.getSecond();
    }

    public long[] computeAllAggregatesOfNaturalJoin() {
        result = new long[distinctPairs.size()];

        while (!trieJoin.atEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            int agg = 0;
            for (int[] instruction : distinctPairs)
                result[agg++] += calculateFromInstruction(instruction, tuple);
            trieJoin.next();
        }

        return result;
    }

    private long calculateFromInstruction(int[] instruction, long[][] tuple) {
        return tuple[instruction[0]][instruction[1]] * tuple[instruction[2]][instruction[3]];
    }

    public long computeOneAggregateOfNaturalJoin() {
        long result = 0;

        while (!trieJoin.atEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            result += calculateFromInstruction(distinctPairs.get(0), tuple);
            trieJoin.next();
        }

        return result;
    }
}

