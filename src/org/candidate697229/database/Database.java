package org.candidate697229.database;

import org.candidate697229.util.ImmutablePair;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static org.candidate697229.config.Configuration.USE_TEST_TABLE;

public class Database {
    private ArrayList<Relation> relations;

    public Database(ArrayList<Relation> relations) {
        this.relations = relations;
    }

    private void readFromDirectory(String directoryName) {
        relations.forEach(relation -> relation.readFromFile(new File(directoryName, relation.getName() + ".tbl")));
    }

    public ArrayList<Relation> getRelations() {
        return relations;
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

    private static ArrayList<Relation> housingTables() {
        ArrayList<Relation> relations = new ArrayList<>(6);
        relations.add(new Relation("House", Arrays.asList("postcode", "area", "price", "bedrooms", "bathrooms",
                "kitchen", "house", "flat", "condo", "garden", "parking")));
        relations.add(new Relation("Shop", Arrays.asList("postcode", "openinghoursshop", "pricerangeshop",
                "sainsburys", "tesco", "ms")));
        relations.add(new Relation("Institution", Arrays.asList("postcode", "typeeducation", "sizeinstitution")));
        relations.add(new Relation("Restaurant", Arrays.asList("postcode", "openinghoursrest", "pricerangerest")));
        relations.add(new Relation("Demographics", Arrays.asList("postcode", "averagesalary", "crimesperyear",
                "unemployment", "nbhospitals")));
        relations.add(new Relation("Transport", Arrays.asList("postcode", "nbbuslines", "nbtrainstations",
                "distancecitycentre")));
        return relations;
    }

    private static ArrayList<Relation> testTables() {
        ArrayList<Relation> relations = new ArrayList<>(4);
        relations.add(new Relation("R1", Arrays.asList("A","B","C")));
        relations.add(new Relation("R2", Arrays.asList("A","B","D")));
        relations.add(new Relation("R3", Arrays.asList("A","E")));
        relations.add(new Relation("R4", Arrays.asList("E","F")));
        return relations;
    }

    public List<ImmutablePair<String, String>> getAttributeNamePairs() {
        List<String> attributes = getRelations().stream()
                .flatMap(relation -> relation.getAttributes().stream())
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

    public List<int[]> getAllPairsOfColums() {
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
        for (int i = 0; i < relations.size(); ++i) {
            List<String> attributes = relations.get(i).getAttributes();
            for (int j = 0; j < attributes.size(); ++j) {
                if (seenWhere.containsKey(attributes.get(j)))
                    seenWhere.get(attributes.get(j)).add(new int[]{i, j});
                else
                    seenWhere.put(attributes.get(j), new LinkedList<>(Collections.singleton(new int[]{i, j})));
            }
        }
        return seenWhere;
    }

    public List<List<int[]>> getAllExplicitJoinConditions() {
        return findAttributePositions().values().stream().sorted((variable1, variable2) -> {
            for (int[] positionOne : variable1) {
                for (int[] positionTwo : variable2) {
                    if (positionOne[0] == positionTwo[0])
                        return Integer.compare(positionOne[1], positionTwo[1]);
                }
            }
            return Integer.compare(variable2.size(), variable1.size());
        }).collect(Collectors.toList());
    }
}
