package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.join.LeapfrogTriejoin;
import org.candidate697229.structures.Iterator;

import static org.candidate697229.config.Configuration.USE_TEST_DATABASE;

/**
 * Implementation of aggregation with the first improvement.
 */
public class AggOne implements AggAlgorithm {
    private final int[][] distinctPairs;
    private final LeapfrogTriejoin leapfrogTriejoin;
    private final Iterator[] iterators;
    private int[] returnPositions;

    /**
     * Construction a new instance of this algorithm.
     * @param scaleFactor the scaleFactor to run on
     */
    public AggOne(int scaleFactor) {
        Database database = Database.makeFromDirectory(USE_TEST_DATABASE ? "test-table" : "housing/housing-" + scaleFactor);
        distinctPairs = database.getAllPairsOfAttributes().toArray(new int[0][]);
        leapfrogTriejoin = new LeapfrogTriejoin(database, database.getAllExplicitJoinConditions());
        iterators = leapfrogTriejoin.getIterators();
        returnPositions = new int[iterators.length];
        leapfrogTriejoin.init();
    }

    @Override
    public long[] computeAllAggregatesOfNaturalJoin() {
        long[] result = new long[distinctPairs.length];

        while (!leapfrogTriejoin.overallAtEnd()) {
            long[][] tuple = leapfrogTriejoin.resultTuple();
            int agg = 0;
            for (int[] distinctPair : distinctPairs)
                result[agg++] += calculateFromInstruction(distinctPair, tuple);
            advanceToNextTuple();
        }

        return result;
    }

    @Override
    public long computeOneAggregateOfNaturalJoin() {
        long result = 0;

        while (!leapfrogTriejoin.overallAtEnd()) {
            result += calculateFromInstruction(distinctPairs[0], leapfrogTriejoin.resultTuple());
            advanceToNextTuple();
        }

        return result;
    }

    /**
     * Advance the iterators to the next tuple, by either iterating in tuple-nested loop join style over tuples with
     * the same join keys, or using the leapfrog triejoin to move to the next matching join keys if we have exhaused
     * all tuples with the current join keys.
     */
    private void advanceToNextTuple() {
        for (int i = 0; i < iterators.length; ++i) {
            if (iterators[i].isNextInBlock()) {
                /*
                 * If we reach here, i is the first iterator that we can advance without changing the join keys, so we
                 * do this and then rewind any iterators before this to get all combinations with tuples from these.
                 */
                iterators[i].nextInBlock();
                returnPositions[i]++;
                for (int j = 0; j < i; ++j)
                    rewind(j);
                return;
            }
        }

        /*
         * If we reach here, we have finished iterating over combinations of tuples with the current join key, and should
         * instead use leapfrog triejoin to jump to the next join key in the data (after rewinding all the iterators).
         */
        for (int i = 0; i < iterators.length; ++i)
            rewind(i);
        leapfrogTriejoin.overallNext();
    }

    /**
     * Move iterator i back to the start of the current block of tuples with the same join keys.
     * @param i the iterator to move back
     */
    private void rewind(int i) {
        iterators[i].back(returnPositions[i]);
        returnPositions[i] = 0;
    }

    /**
     * Calculate a product from a set of result tuples of the join.
     * @param instruction a four-tuple of relation and position in the relation for two attributes we wish to multiply
     * @param tuples the result to tuples to look in
     * @return the result of the multiplication
     */
    private long calculateFromInstruction(int[] instruction, long[][] tuples) {
        return tuples[instruction[0]][instruction[1]] * tuples[instruction[2]][instruction[3]];
    }
}

