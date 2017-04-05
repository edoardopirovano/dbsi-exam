package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Relation;
import org.candidate697229.join.LeapfrogTriejoin;
import org.candidate697229.structures.Iterator;
import org.candidate697229.structures.SequentialIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.candidate697229.util.Configuration.USE_TEST_DATABASE;

/**
 * Implementation of aggregation with both improvements.
 */
public class AggTwo implements AggAlgorithm {
    private final List<int[]> instructions;
    private final int[] numberOfJoinAttributes;
    private final LeapfrogTriejoin leapfrogTriejoin;
    private final long[][] summedTuple;
    private final Iterator[] iterators;

    /**
     * Construct a new instance of this algorithm.
     * @param scaleFactor the scaleFactor to run on
     */
    public AggTwo(int scaleFactor) {
        Database database = Database.makeFromDirectory(USE_TEST_DATABASE ? "test-table" : "housing/housing-" + scaleFactor);
        instructions = getInstructionsForSummedDatabase(database);
        numberOfJoinAttributes = new int[database.getRelations().size()];
        summedTuple = new long[database.getRelations().size()][];
        iterators = new Iterator[database.getRelations().size()];
        for (int i = 0; i < database.getRelations().size(); ++i)
            iterators[i] = new SequentialIterator(database.getRelations().get(i).getTuples());
        leapfrogTriejoin = new LeapfrogTriejoin(iterators, database.getAllExplicitJoinConditions());
        for (int i = 0; i < database.getRelations().size(); ++i) {
            Relation relation = database.getRelations().get(i);
            List<List<int[]>> joinInstructions = database.getAllExplicitJoinConditions().stream()
                    .filter(instructions -> instructions.size() > 1).collect(Collectors.toList());
            for (List<int[]> positions : joinInstructions) {
                for (int[] position : positions) {
                    if (position[0] == i)
                        numberOfJoinAttributes[i]++;
                }
            }
            int numOfAttributes = relation.getAttributes().size();
            summedTuple[i] = new long[numberOfJoinAttributes[i] + 1 + numOfAttributes +
                    ((numOfAttributes * (numOfAttributes + 1)) / 2)];
        }
    }

    @Override
    public long[] computeAllAggregatesOfNaturalJoin() {
        long[] result = new long[instructions.size()];
        while (!leapfrogTriejoin.overallAtEnd()) {
            calculateSummedTuple();
            long countProduct = 1;
            for (int i = 0; i < summedTuple.length; ++i)
                countProduct *= summedTuple[i][numberOfJoinAttributes[i]];
            int pos = 0;
            for (int[] instruction : instructions)
                result[pos++] += calculateFromInstruction(instruction, countProduct);
            leapfrogTriejoin.overallNext();
        }
        return result;
    }

    @Override
    public long computeOneAggregateOfNaturalJoin() {
        long result = 0;
        while (!leapfrogTriejoin.overallAtEnd()) {
            calculateSummedTuple();
            long countProduct = 1;
            for (int i = 0; i < summedTuple.length; ++i)
                countProduct *= summedTuple[i][numberOfJoinAttributes[i]];
            result += calculateFromInstruction(instructions.get(0), countProduct);
            leapfrogTriejoin.overallNext();
        }
        return result;
    }

    /**
     * For each relation, iterate over the tuples with the current join key, and compute the partial aggregates we will
     * need into the two-dimensional array summedTuple.
     */
    private void calculateSummedTuple() {
        for (int i = 0; i < iterators.length; ++i) {
            /*
             * Copy the join key into the start of the summed tuple, recording if it has changed or not
             */
            boolean didChangeJoinKey = false;
            for (int j = 0; j < numberOfJoinAttributes[i]; ++j) {
                if (summedTuple[i][j] != iterators[i].value()[j]) {
                    summedTuple[i][j] = iterators[i].value()[j];
                    didChangeJoinKey = true;
                }
            }

            /*
             * If the join key hasn't changed for this relation, we needn't recompute the partial aggregates,
             * so we can just move on to the next relation.
             */
            if (!didChangeJoinKey)
                continue;

            /*
             * Reset the COUNT and all the SUMs to 0.
             */
            for (int j = numberOfJoinAttributes[i]; j < summedTuple[i].length; ++j)
                summedTuple[i][j] = 0;

            while (true) {
                long[] tuple = iterators[i].value();
                int k = numberOfJoinAttributes[i];

                /*
                 * Increment the COUNT by one.
                 */
                summedTuple[i][k++]++;

                /*
                 * Adjust all aggregates of the form SUM(A) for a single attribute A.
                 */
                for (long attribute : tuple) summedTuple[i][k++] += attribute;


                /*
                 * Adjust all aggregates of the form SUM(A*B) for a pair of attributes A and B, with A preceding or equal to B.
                 */
                for (int a = 0; a < tuple.length; a++) {
                    for (int b = a; b < tuple.length; b++)
                        summedTuple[i][k++] += tuple[a] * tuple[b];
                }

                if (!iterators[i].isNextInBlock())
                    break;
                iterators[i].nextInBlock();
            }
        }
    }

    /**
     * Calculate the adjustment to make to an overall aggregate from an instruction.
     * The instruction is an array that can take one of two forms:
     * - It begins with a 0, then has a pair consisting of the two dimensions of the position in the summed tuple
     * to multiply the count by after dividing it by the count in the relation this position corresponds to.
     * - It begins with a 1, then has two pairs, giving the two positions in the summed tuple to multiply the
     * count by after dividing it by the count in both relations these positions point to.
     *
     * @param instruction  the instruction to use to calculate the adjustment from the partial aggregates
     * @param countProduct the product of all the COUNT aggregates for the current join key
     * @return the adjustment to make to an overall aggregate
     */
    private long calculateFromInstruction(int[] instruction, long countProduct) {
        if (instruction[0] == 0)
            return (countProduct / summedTuple[instruction[1]][numberOfJoinAttributes[instruction[1]]]) *
                    summedTuple[instruction[1]][instruction[2] + numberOfJoinAttributes[instruction[1]]];
        return ((countProduct / summedTuple[instruction[1]][numberOfJoinAttributes[instruction[1]]])
                / summedTuple[instruction[3]][numberOfJoinAttributes[instruction[3]]])
                * summedTuple[instruction[1]][instruction[2] + numberOfJoinAttributes[instruction[1]]]
                * summedTuple[instruction[3]][instruction[4] + numberOfJoinAttributes[instruction[3]]];
    }

    /**
     * Get a list of instructions for calculating overall aggregates from the partial aggregates in a given database.
     * @param database the database to consider
     * @return a list of instructions
     *          (for a description of the form these instructions take, see the calculateFromInstruction method above)
     */
    private List<int[]> getInstructionsForSummedDatabase(Database database) {
        List<int[]> distinctPairs = database.getAllPairsOfAttributes();
        List<int[]> instructions = new ArrayList<>();

        for (int[] pair : distinctPairs) {
            int[] instruction;
            if (pair[0] == pair[2])
                instruction = new int[]{0, pair[0], 1 +
                        database.getRelations().get(pair[0]).getAttributes().size() +
                        calculatePosition(pair[1], database.getRelations().get(pair[0]).getAttributes().size()) +
                        (pair[3] - pair[1])};
            else
                instruction = new int[]{1, pair[0], 1 + pair[1], pair[2], 1 + pair[3]};
            instructions.add(instruction);
        }

        return instructions;
    }

    /**
     * Calculate the position at which the aggregate SUM(A*A) can be found in the tuple of partial aggregates.
     * @param k the index of the relation A
     * @param relationSize the size of the relation
     * @return the position in the summed tuple for the relation at which SUM(A*A) can be found
     */
    private int calculatePosition(int k, int relationSize) {
        int result = 0;
        for (int i = 0; i < k; ++i)
            result += (relationSize - i);
        return result;
    }
}
