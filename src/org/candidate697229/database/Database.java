package org.candidate697229.database;

import org.candidate697229.util.ImmutablePair;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static org.candidate697229.util.Configuration.USE_EXAMPLE_DATABASE;

/**
 * Class representing a database (set of relations).
 */
public class Database {
    private List<Relation> relations;

    /**
     * Create a database.
     * @param relations the relations in the database (need not be populate with tuples yet)
     */
    private Database(List<Relation> relations) {
        this.relations = relations;
    }

    /**
     * Make a database from a directory, including populating it with tuples.
     * @param directoryName the directory to read in the database from
     * @return the database read in
     */
    public static Database makeFromDirectory(String directoryName) {
        return makeFromDirectory(directoryName, true);
    }

    /**
     * Make a database from a directory.
     * @param directoryName the directory to read in the database from
     * @param shouldPopulate whether or not to read in tuples (if not, returns an empty database wwith the correct schema)
     * @return the database read in
     */
    public static Database makeFromDirectory(String directoryName, boolean shouldPopulate) {
        Database database = new Database(USE_EXAMPLE_DATABASE ? exampleRelations() : housingRelations());
        if (shouldPopulate)
            database.readFromDirectory(directoryName);
        return database;
    }

    /**
     * Get the relations in the housing database.
     * @return the relations in the housing database (not populated with tuples)
     */
    private static ArrayList<Relation> housingRelations() {
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

    /**
     * Get the relations in the test database.
     * @return the relations in the test database (not populated with tuples)
     */
    private static ArrayList<Relation> exampleRelations() {
        ArrayList<Relation> relations = new ArrayList<>(4);
        relations.add(new Relation("R1", Arrays.asList("A","B","C")));
        relations.add(new Relation("R2", Arrays.asList("A","B","D")));
        relations.add(new Relation("R3", Arrays.asList("A","E")));
        relations.add(new Relation("R4", Arrays.asList("E","F")));
        return relations;
    }

    /**
     * Read in a database matching the schema in the relations from a directory containing a .tbl for each relation.
     *
     * @param directoryName the directory to read the databse from
     */
    private void readFromDirectory(String directoryName) {
        relations.forEach(relation -> relation.readFromFile(new File(directoryName, relation.getName() + ".tbl")));
    }

    /**
     * Get the relations in the database.
     *
     * @return the list of relations in the database
     */
    public List<Relation> getRelations() {
        return relations;
    }

    /**
     * Get all pairs of attribute names in the database (symmetric pairs appear only once).
     *
     * @return a list of pairs of attribute names
     */
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

    /**
     * Find all pairs of attributes in the database (symmetric pairs appear only once).
     * @return a list of 4-tuples with elements 0 and 1 representing the relation and position within that relation for
     *          the first attribute in the pair, and elements 2 and 3 representing the same for the second attribute
     */
    public List<int[]> getAllPairsOfAttributes() {
        HashMap<String, List<int[]>> seenWhere = findAttributePositions();
        List<int[]> distinctAttributes = seenWhere.values().stream().map(x -> x.get(0)).collect(Collectors.toList());

        List<int[]> attributePairs = new ArrayList<>();
        for (int j = 0; j < distinctAttributes.size(); ++j) {
            int[] firstAttribute = distinctAttributes.get(j);
            for (int k = j; k < distinctAttributes.size(); ++k) {
                int[] secondAttribute = distinctAttributes.get(k);
                attributePairs.add(new int[]{firstAttribute[0], firstAttribute[1],
                            secondAttribute[0], secondAttribute[1]});
            }
        }

        return attributePairs;
    }

    /**
     * Find where each attribute appears in the database.
     * @return a map from each attribute name to a list of positions it appears in, where each position takes the form
     *          of a pair of integer giving the relation and attribute within that relation
     */
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

    /**
     * Get the explicit conditions in the natural join of the all the relations in this database.
     * @return the explicit join conditions, as a list of lists of pairs, with each pair of integers representing a
     *          relation and attribute within that relation that should be equal to all others in the same list
     */
    public List<List<int[]>> getAllExplicitJoinConditions() {
        return findAttributePositions().values().stream()
                .filter(positions -> positions.size() > 1)
                .sorted((positionsOne, positionsTwo) -> {
                    for (int[] positionOne : positionsOne) {
                        for (int[] positionTwo : positionsTwo) {
                            if (positionOne[0] == positionTwo[0])
                                return Integer.compare(positionOne[1], positionTwo[1]);
                        }
                    }
                    return Integer.compare(positionsTwo.size(), positionsOne.size());
            }).collect(Collectors.toList());
    }
}
