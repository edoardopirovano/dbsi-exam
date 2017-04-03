package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Relation;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class AggTwo {
    private final List<int[]> instructions;
    private final int[] numberOfJoinAttributes;
    private final TrieJoin trieJoin;

    public AggTwo(Database database) {
        instructions = getInstructionsForSummedDatabase(database);
        numberOfJoinAttributes = new int[database.getRelations().size()];
        trieJoin = new TrieJoin(computeSumDatabase(database), database.getAllExplicitJoinConditions());
        trieJoin.init();
    }

    public long[] computeAllAggregatesOfNaturalJoin() {
        long[] result = new long[instructions.size()];
        while (!trieJoin.overallAtEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            long countProduct = 1;
            for (int i = 0; i < tuple.length; ++i)
                countProduct *= tuple[i][numberOfJoinAttributes[i]];
            int pos = 0;
            for (int[] instruction : instructions)
                result[pos++] += calculateFromInstruction(instruction, tuple, countProduct);
            trieJoin.overallNext();
        }
        return result;
    }

    private long calculateFromInstruction(int[] instruction, long[][] tuple, long countProduct) {
        if (instruction[0] == 0)
            return (countProduct / tuple[instruction[1]][numberOfJoinAttributes[instruction[1]]]) *
                    tuple[instruction[1]][instruction[2] + numberOfJoinAttributes[instruction[1]]];
        return ((countProduct / tuple[instruction[1]][numberOfJoinAttributes[instruction[1]]])
                / tuple[instruction[3]][numberOfJoinAttributes[instruction[3]]])
                * tuple[instruction[1]][instruction[2] + numberOfJoinAttributes[instruction[1]]]
                * tuple[instruction[3]][instruction[4] + numberOfJoinAttributes[instruction[3]]];
    }

    public long computeOneAggregateOfNaturalJoin() {
        long result = 0;
        while (!trieJoin.overallAtEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            long countProduct = 1;
            for (int i = 0; i < tuple.length; ++i)
                countProduct *= tuple[i][numberOfJoinAttributes[i]];
            result += calculateFromInstruction(instructions.get(0), tuple, countProduct);
            trieJoin.overallNext();
        }
        return result;
    }

    private List<int[]> getInstructionsForSummedDatabase(Database database) {
        List<int[]> distinctPairs = database.getAllPairsOfColums();
        List<int[]> instructions = new ArrayList<>();

        for (int[] pair : distinctPairs) {
            int[] instruction;
            if (pair[0] == pair[2])
                instruction = new int[]{0, pair[0], 1 +
                        database.getRelations().get(pair[0]).getAttributes().size() +
                        calculatePosition(pair[1], pair[0], database.getRelations().get(pair[0]).getAttributes().size()) +
                        (pair[3] - pair[1])};
            else
                instruction = new int[]{1, pair[0], 1 + pair[1], pair[2], 1 + pair[3]};
            instructions.add(instruction);
        }

        return instructions;
    }

    private int calculatePosition(int k, int table, int tableSize) {
        int result = 0;
        for (int i = 0; i < k; ++i)
            result += (tableSize - i);
        return result;
    }

    private Database computeSumDatabase(Database database) {
        ArrayList<Relation> summedRelations = new ArrayList<>(database.getRelations().size());
        int k = 0;
        for (Relation relation : database.getRelations()) {
            List<String> newAttributes = new ArrayList<>();
            List<List<int[]>> joinInstructions = database.getAllExplicitJoinConditions().stream()
                    .filter(instructions -> instructions.size() > 1).collect(Collectors.toList());
            ArrayList<Integer> joinAttributes = new ArrayList<>();
            for (List<int[]> positions : joinInstructions) {
                for (int[] position : positions) {
                    if (position[0] == k)
                        joinAttributes.add(position[1]);
                }
            }
            numberOfJoinAttributes[k] = joinAttributes.size();
            joinAttributes.forEach(position -> newAttributes.add(relation.getAttributes().get(position)));
            newAttributes.add("COUNT(" + relation.getName() + ")");
            newAttributes.addAll(relation.getAttributes().stream()
                    .map(attribute -> "SUM(" + attribute + ")")
                    .collect(Collectors.toList())
            );
            List<int[]> pairs = new LinkedList<>();
            for (int i = 0; i < relation.getAttributes().size(); ++i) {
                for (int j = i; j < relation.getAttributes().size(); ++j)
                    pairs.add(new int[]{i, j});
            }
            newAttributes.addAll(pairs.stream()
                    .map(pair -> "SUM(" + relation.getAttributes().get(pair[0]) + "*" + relation.getAttributes().get(pair[1]) + ")")
                    .collect(Collectors.toList())
            );
            Relation summedRelation = new Relation(relation.getName(), newAttributes);
            LinkedList<long[]> tuples = new LinkedList<>();
            long[] lastJoinKey = new long[joinAttributes.size()];
            long[] currentTuple = new long[newAttributes.size()];
            copyJoinKeys(relation.getTuples()[0], lastJoinKey, currentTuple, joinAttributes);
            for (long[] tuple : relation.getTuples()) {
                if (compareJoinKeys(tuple, lastJoinKey, joinAttributes)) {
                    tuples.add(currentTuple);
                    currentTuple = new long[currentTuple.length];
                    copyJoinKeys(tuple, lastJoinKey, currentTuple, joinAttributes);
                }
                ++currentTuple[joinAttributes.size()];
                for (int i = 0; i < tuple.length; ++i)
                    currentTuple[i + 1 + joinAttributes.size()] += tuple[i];
                int pos = tuple.length + 1 + joinAttributes.size();
                for (int[] pair : pairs)
                    currentTuple[pos++] += tuple[pair[0]] * tuple[pair[1]];
            }
            tuples.add(currentTuple);
            long[][] newTuples = new long[tuples.size()][];
            int i = 0;
            for (long[] tuple : tuples)
                newTuples[i++] = tuple;
            summedRelation.putTuples(newTuples);
            summedRelations.add(summedRelation);
            ++k;
        }
        return new Database(summedRelations);
    }

    private boolean compareJoinKeys(long[] tuple, long[] lastJoinKey, List<Integer> joinAttributes) {
        for (int i : joinAttributes) {
            if (tuple[i] != lastJoinKey[i])
                return true;
        }
        return false;
    }

    private void copyJoinKeys(long[] tuple, long[] lastJoinKey, long[] currentTuple, List<Integer> joinKeys) {
        for (int i : joinKeys) {
            lastJoinKey[i] = tuple[i];
            currentTuple[i] = tuple[i];
        }
    }
}
