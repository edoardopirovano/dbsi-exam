package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.join.LeapfrogTriejoin;
import org.candidate697229.structures.Iterator;
import org.candidate697229.structures.SequentialIterator;

import static org.candidate697229.util.Configuration.USE_TEST_DATABASE;

/**
 * Implementation of aggregation with the first improvement.
 */
public class AggOne implements AggAlgorithm {
    private final int[][] attributePairs;
    private final LeapfrogTriejoin leapfrogTriejoin;
    private final Iterator[] iterators;
    private int[] returnPositions;
    private long[][] tuples;

    /**
     * Construction a new instance of this algorithm.
     * @param scaleFactor the scaleFactor to run on
     */
    public AggOne(int scaleFactor) {
        Database database = Database.makeFromDirectory(USE_TEST_DATABASE ? "test-table" : "housing/housing-" + scaleFactor);
        attributePairs = database.getAllPairsOfAttributes().toArray(new int[0][]);
        iterators = new Iterator[database.getRelations().size()];
        for (int i = 0; i < database.getRelations().size(); ++i)
            iterators[i] = new SequentialIterator(database.getRelations().get(i).getTuples());
        leapfrogTriejoin = new LeapfrogTriejoin(iterators, database.getAllExplicitJoinConditions());
        returnPositions = new int[iterators.length];
        tuples = new long[iterators.length][];
    }

    @Override
    public long[] computeAllAggregatesOfNaturalJoin() {
        long[] result = new long[attributePairs.length];

        copyAllTuples();
        while (!leapfrogTriejoin.overallAtEnd()) {
            int agg = 0;
            for (int[] attributePair : attributePairs)
                result[agg++] += calculateFromInstruction(attributePair);
            advanceToNextTuple();
        }

        return result;
    }

    @Override
    public long computeOneAggregateOfNaturalJoin() {
        long result = 0;

        copyAllTuples();
        while (!leapfrogTriejoin.overallAtEnd()) {
            result += calculateFromInstruction(attributePairs[0]);
            advanceToNextTuple();
        }

        return result;
    }

    /**
     * Copy all the tuples at the current position of each iterator to the tuples variable.
     */
    private void copyAllTuples() {
        for (int i = 0; i < iterators.length; ++i)
            tuples[i] = iterators[i].value();
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
                tuples[i] = iterators[i].value();
                returnPositions[i]++;
                for (int j = 0; j < i; ++j) {
                    iterators[j].back(returnPositions[j]);
                    returnPositions[j] = 0;
                    tuples[j] = iterators[j].value();
                }
                return;
            }
        }

        /*
         * If we reach here, we have finished iterating over combinations of tuples with the current join key, and should
         * instead use leapfrog triejoin to jump to the next join key in the data (after rewinding all the iterators).
         */
        for (int i = 0; i < iterators.length; ++i)
            returnPositions[i] = 0;
        leapfrogTriejoin.overallNext();
        copyAllTuples();
    }

    /**
     * Calculate a product from a set of result tuples of the join.
     * @param instruction a four-tuple of relation and position in the relation for two attributes we wish to multiply
     * @return the result of the multiplication
     */
    private long calculateFromInstruction(int[] instruction) {
        return tuples[instruction[0]][instruction[1]] * tuples[instruction[2]][instruction[3]];
    }
}

