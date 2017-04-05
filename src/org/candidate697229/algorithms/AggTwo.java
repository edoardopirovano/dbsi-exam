package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Relation;
import org.candidate697229.join.LeapfrogTriejoin;
import org.candidate697229.structures.Iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.candidate697229.config.Configuration.USE_TEST_DATABASE;

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
        leapfrogTriejoin = new LeapfrogTriejoin(database, database.getAllExplicitJoinConditions());
        iterators = leapfrogTriejoin.getIterators();
        leapfrogTriejoin.init();
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

    private void calculateSummedTuple() {
        for (int i = 0; i < iterators.length; ++i) {
            boolean didChangeJoinKey = false;
            for (int j = 0; j < numberOfJoinAttributes[i]; ++j) {
                if (summedTuple[i][j] != iterators[i].value()[j]) {
                    summedTuple[i][j] = iterators[i].value()[j];
                    didChangeJoinKey = true;
                }
            }
            if (!didChangeJoinKey)
                continue;

            for (int j = numberOfJoinAttributes[i]; j < summedTuple[i].length; ++j)
                summedTuple[i][j] = 0;
            while (true) {
                long[] tuple = iterators[i].value();
                int k = numberOfJoinAttributes[i];

                summedTuple[i][k++]++;

                for (long attribute : tuple) summedTuple[i][k++] += attribute;

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

    private long calculateFromInstruction(int[] instruction, long countProduct) {
        if (instruction[0] == 0)
            return (countProduct / summedTuple[instruction[1]][numberOfJoinAttributes[instruction[1]]]) *
                    summedTuple[instruction[1]][instruction[2] + numberOfJoinAttributes[instruction[1]]];
        return ((countProduct / summedTuple[instruction[1]][numberOfJoinAttributes[instruction[1]]])
                / summedTuple[instruction[3]][numberOfJoinAttributes[instruction[3]]])
                * summedTuple[instruction[1]][instruction[2] + numberOfJoinAttributes[instruction[1]]]
                * summedTuple[instruction[3]][instruction[4] + numberOfJoinAttributes[instruction[3]]];
    }

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

    private int calculatePosition(int k, int tableSize) {
        int result = 0;
        for (int i = 0; i < k; ++i)
            result += (tableSize - i);
        return result;
    }
}
