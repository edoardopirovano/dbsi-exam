package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Table;
import org.candidate697229.util.ImmutablePair;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class AggTwo {
    private final Database database;
    private final TrieJoin trieJoin;
    private final List<int[]> instructions;

    public AggTwo(Database database) {
        this.database = computeSumDatabase(database);
        ImmutablePair<List<int[]>, List<int[]>> conditionAndInstructions = database.getConditionsAndInstructions();
        trieJoin = new TrieJoin(this.database, conditionAndInstructions.getFirst());
        trieJoin.init();
        instructions = conditionAndInstructions.getSecond();
    }

    public long[] computeAllAggregatesOfNaturalJoin() {
        long[] result = new long[instructions.size()];
        while (!trieJoin.atEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            long countProduct = 1;
            for (long[] aTuple : tuple) countProduct *= aTuple[1];
            int pos = 0;
            for (int[] instruction : instructions)
                result[pos++] += calculateFromInstruction(instruction, tuple, countProduct);
            trieJoin.next();
        }
        return result;
    }

    private long calculateFromInstruction(int[] instruction, long[][] tuple, long countProduct) {
        if (instruction[0] == 0)
            return (countProduct / tuple[instruction[1]][1]) * tuple[instruction[1]][instruction[2]];
        return ((countProduct / tuple[instruction[1]][1]) / tuple[instruction[3]][1])
                * tuple[instruction[1]][instruction[2]] * tuple[instruction[3]][instruction[4]];
    }

    public long computeOneAggregateOfNaturalJoin() {
        long result = 0;
        while (!trieJoin.atEnd()) {
            long[][] tuple = trieJoin.resultTuple();
            long countProduct = 1;
            for (long[] aTuple : tuple) countProduct *= aTuple[1];
            result += calculateFromInstruction(instructions.get(0), tuple, countProduct);
            trieJoin.next();
        }
        return result;
    }

    private Database computeSumDatabase(Database database) {
        ArrayList<Table> summedTables = new ArrayList<>(database.getTables().size());
        for (Table table : database.getTables()) {
            List<String> newAttributes = new ArrayList<>();
            newAttributes.add(table.getAttributes().get(0));
            newAttributes.add("COUNT(" + table.getName() + ")");
            newAttributes.addAll(table.getAttributes().stream()
                    .map(attribute -> "SUM(" + attribute + ")")
                    .collect(Collectors.toList())
            );
            List<int[]> pairs = new LinkedList<>();
            for (int i = 0; i < table.getAttributes().size(); ++i) {
                for (int j = i; j < table.getAttributes().size(); ++j)
                    pairs.add(new int[]{i, j});
            }
            newAttributes.addAll(pairs.stream()
                    .map(pair -> "SUM(" + table.getAttributes().get(pair[0]) + "*" + table.getAttributes().get(pair[1]) + ")")
                    .collect(Collectors.toList())
            );
            Table summedTable = new Table(table.getName(), newAttributes);
            LinkedList<long[]> tuples = new LinkedList<>();
            long lastJoinKey = table.getTuples()[0][0];
            long[] currentTuple = new long[newAttributes.size()];
            currentTuple[0] = lastJoinKey;
            for (long[] tuple : table.getTuples()) {
                if (tuple[0] != lastJoinKey) {
                    tuples.add(currentTuple);
                    currentTuple = new long[currentTuple.length];
                    lastJoinKey = tuple[0];
                    currentTuple[0] = lastJoinKey;
                }
                ++currentTuple[1];
                for (int i = 0; i < tuple.length; ++i)
                    currentTuple[i + 2] += tuple[i];
                int pos = tuple.length + 2;
                for (int[] pair : pairs)
                    currentTuple[pos++] += tuple[pair[0]] * tuple[pair[1]];
            }
            tuples.add(currentTuple);
            long[][] newTuples = new long[tuples.size()][];
            int i = 0;
            for (long[] tuple : tuples)
                newTuples[i++] = tuple;
            summedTable.putTuples(newTuples);
            summedTables.add(summedTable);
        }
        return new Database(summedTables);
    }
}
