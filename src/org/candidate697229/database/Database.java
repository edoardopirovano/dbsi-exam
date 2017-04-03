package org.candidate697229.database;

import org.candidate697229.util.ImmutablePair;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static org.candidate697229.config.Configuration.USE_TEST_TABLE;

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
        Database database = new Database(USE_TEST_TABLE ? testTables() : housingTables());
        if (shouldPopulate)
            database.readFromDirectory(directoryName);
        return database;
    }

    private static ArrayList<Table> housingTables() {
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
        return tables;
    }

    private static ArrayList<Table> testTables() {
        ArrayList<Table> tables = new ArrayList<>(4);
        tables.add(new Table("R1", Arrays.asList("A","B","C")));
        tables.add(new Table("R2", Arrays.asList("A","B","D")));
        tables.add(new Table("R3", Arrays.asList("A","E")));
        tables.add(new Table("R4", Arrays.asList("E","F")));
        return tables;
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

    private HashMap<String, List<int[]>> findAttributePositions() {
        HashMap<String, List<int[]>> seenWhere = new LinkedHashMap<>();
        for (int i = 0; i < tables.size(); ++i) {
            List<String> attributes = tables.get(i).getAttributes();
            for (int j = 0; j < attributes.size(); ++j) {
                if (seenWhere.containsKey(attributes.get(j)))
                    seenWhere.get(attributes.get(j)).add(new int[]{i, j});
                else
                    seenWhere.put(attributes.get(j), new LinkedList<>(Collections.singleton(new int[]{i, j})));
            }
        }
        return seenWhere;
    }

    public List<List<int[]>> getJoinInstructions() {
        return findAttributePositions().values().stream().filter(positions -> positions.size() > 1).sorted(new Comparator<List<int[]>>() {
            @Override
            public int compare(List<int[]> joinCondition1, List<int[]> joinCondition2) {
                return Integer.compare(getMinPosInTable(joinCondition1), getMinPosInTable(joinCondition2));
            }

            private int getMinPosInTable(List<int[]> joinCondition) {
                //noinspection OptionalGetWithoutIsPresent
                return joinCondition.stream().mapToInt(pos -> pos[1]).min().getAsInt();
            }
        }).collect(Collectors.toList());
    }

    public List<int[]> getInstructionsForSummedDatabase() {
        List<int[]> distinctPairs = getAllPairs();
        List<int[]> instructions = new ArrayList<>();

        int[] joinAttributesIn = new int[tables.size()];
        getJoinInstructions().forEach(joinCondition -> joinCondition.forEach(joinPos -> joinAttributesIn[joinPos[0]]++));

        for (int[] pair : distinctPairs) {
            int[] instruction;
            if (pair[0] == pair[2])
                instruction = new int[]{0, pair[0], 1 +
                        joinAttributesIn[pair[0]] +
                        tables.get(pair[0]).getAttributes().size() +
                        calculatePosition(pair[1], pair[0]) +
                        (pair[3] - pair[1])};
            else
                instruction = new int[]{1, pair[0], pair[1] + 1 + joinAttributesIn[pair[0]], pair[2], pair[3] + 1 + joinAttributesIn[pair[2]]};
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
