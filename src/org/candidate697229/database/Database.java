package org.candidate697229.database;

import org.candidate697229.util.ImmutablePair;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class Database {
    private ArrayList<Table> tables;

    private Database(ArrayList<Table> tables) {
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

    public ImmutablePair<List<int[]>, List<int[]>> getConditionsAndDistinct() {
        List<int[]> joinConditions = new ArrayList<>();
        List<int[]> distinctAttributes = new ArrayList<>();

        HashMap<String, List<int[]>> seenWhere = new LinkedHashMap<>();
        for (int i = 0; i < tables.size(); ++i) {
            List<String> attributes = tables.get(i).getAttributes();
            for (int j = 0; j < attributes.size(); ++j) {
                if (seenWhere.containsKey(attributes.get(j)))
                    seenWhere.get(attributes.get(j)).add(new int[]{i, j});
                else {
                    seenWhere.put(attributes.get(j), new LinkedList<>(Collections.singleton(new int[]{i, j})));
                    distinctAttributes.add(new int[]{i, j});
                }
            }
        }

        seenWhere.values().stream().filter(positions -> positions.size() > 1).forEach(positions -> {
            int[] first = positions.remove(0);
            for (int[] other : positions)
                joinConditions.add(new int[]{first[0], first[1], other[0], other[1]});
        });

        return new ImmutablePair<>(joinConditions, distinctAttributes);
    }
}
