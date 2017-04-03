package org.candidate697229.database;

import org.candidate697229.util.ImmutablePair;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class Database {
    private ArrayList<Table> tables;

    public Database(ArrayList<Table> tables) {
        this.tables = tables;
    }

    private void readFromDirectory(String directoryName) {
        tables.forEach(table -> table.readFromFile(new File(directoryName, table.getName() + ".tbl")));
    }

    public ArrayList<Table> getTables() {
        return tables;
    }

    public static Database makeFromDirectory(String directoryName) {
        return makeFromDirectory(directoryName, true);
    }

    public static Database makeFromDirectory(String directoryName, boolean shouldPopulate) {
        ArrayList<Table> tables = new ArrayList<>(6);
        tables.add(new Table("House", Arrays.asList("postcode", "area", "price", "bedrooms", "bathrooms",
                "kitchen", "house", "flat", "condo", "garden", "parking")));
        tables.add(new Table("Shop", Arrays.asList("postcode", "openinghoursshop", "pricerangeshop",
                "sainsburys", "tesco", "ms")));
        tables.add(new Table("Institution", Arrays.asList("postcode", "typeeducation", "sizeinstitution")));
        tables.add(new Table("Restaurant", Arrays.asList("postcode", "openinghoursrest", "pricerangerest")));
        tables.add(new Table("Demographics", Arrays.asList("postcode", "averagesalary", "crimesperyear",
                "unemployment", "nbhospitals")));
        tables.add(new Table("Transport", Arrays.asList("postcode", "nbbuslines", "nbtrainstations",
                "distancecitycentre")));
        Database database = new Database(tables);
        if (shouldPopulate)
            database.readFromDirectory(directoryName);
        return database;
    }

    public List<ImmutablePair<String, String>> getAttributePairs() {
        List<String> attributes = getTables().stream()
                .flatMap(table -> table.getAttributes().stream())
                .distinct()
                .collect(Collectors.toList());

        List<ImmutablePair<String, String>> attributePairs = new LinkedList<>();
        while (attributes.size() > 0) {
            String attribute1 = attributes.get(0);
            attributes.forEach(attribute2 -> attributePairs.add(new ImmutablePair<>(attribute1, attribute2)));
            attributes.remove(0);
        }

        return attributePairs;
    }

    public List<int[]> getAllPairs() {
        HashMap<String, List<int[]>> seenWhere = findAttributePositions();
        List<int[]> distinctAttributes = seenWhere.values().stream().map(x -> x.get(0)).collect(Collectors.toList());
        checkJoinCondition(seenWhere);

        List<int[]> distinctPairs = new ArrayList<>();
        for (int j = 0; j < distinctAttributes.size(); ++j) {
            int[] firstAttribute = distinctAttributes.get(j);
            for (int k = j; k < distinctAttributes.size(); ++k) {
                int[] secondAttribute = distinctAttributes.get(k);
                distinctPairs.add(new int[]{firstAttribute[0], firstAttribute[1],
                            secondAttribute[0], secondAttribute[1]});
            }
        }

        return distinctPairs;
    }

    /*
     Verify that our simplifying assumption that we are joining precisely on the first fields
     of each table does indeed hold.
      */
    private void checkJoinCondition(HashMap<String, List<int[]>> seenWhere) {
        List<int[]> joinConditions = new ArrayList<>();
        seenWhere.values().stream().filter(positions -> positions.size() > 1).forEach(positions -> {
            int[] first = positions.remove(0);
            for (int[] other : positions)
                joinConditions.add(new int[]{first[0], first[1], other[0], other[1]});
        });
        if (joinConditions.size() != tables.size() - 1)
            throw new InternalError("Join condition is not precisely on first column of each table");
        assert (joinConditions.size() == tables.size() - 1);
        for (int i = 0; i < tables.size() - 1; ++i) {
            if (joinConditions.get(i)[0] != 0 || joinConditions.get(i)[1] != 0
                    || joinConditions.get(i)[2] != i + 1 || joinConditions.get(i)[3] != 0)
                throw new InternalError("Join condition is not precisely on first column of each table");
        }
    }

    private HashMap<String, List<int[]>> findAttributePositions() {
        HashMap<String, List<int[]>> seenWhere = new LinkedHashMap<>();
        for (int i = 0; i < tables.size(); ++i) {
            List<String> attributes = tables.get(i).getAttributes();
            for (int j = 0; j < attributes.size(); ++j) {
                if (seenWhere.containsKey(attributes.get(j)))
                    seenWhere.get(attributes.get(j)).add(new int[]{i, j});
                else {
                    seenWhere.put(attributes.get(j), new LinkedList<>(Collections.singleton(new int[]{i, j})));
                }
            }
        }
        return seenWhere;
    }

    public List<int[]> getInstructionsForSummedDatabase() {
        List<int[]> distinctPairs = getAllPairs();
        List<int[]> instructions = new ArrayList<>();

        for (int[] pair : distinctPairs) {
            int[] instruction;
            if (pair[0] == pair[2])
                instruction = new int[]{0, pair[0], 2 + tables.get(pair[0]).getAttributes().size() +
                        calculatePosition(pair[1], pair[0]) + (pair[3] - pair[1])};
            else
                instruction = new int[]{1, pair[0], pair[1] + 2, pair[2], pair[3] + 2};
            instructions.add(instruction);
        }

        return instructions;
    }

    private int calculatePosition(int k, int table) {
        int result = 0;
        for (int i = 0; i < k; ++i)
            result += (tables.get(table).getAttributes().size() - i);
        return result;
    }
}
