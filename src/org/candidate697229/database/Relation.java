package org.candidate697229.database;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;

/**
 * Class representing a relation.
 */
public class Relation {
    private String name;
    private List<String> attributes;
    private long[][] tuples;

    /**
     * Construct a new relation.
     *
     * @param name       the name of the relation
     * @param attributes the name of the attributes in the relation
     */
    Relation(String name, List<String> attributes) {
        this.name = name;
        this.attributes = attributes;
    }

    /**
     * Read in the tuples for this relation from a file which has rows separated by new line characters and attributes
     * within a row separated by pipe characters.
     * @param file the file to read in from
     */
    void readFromFile(File file) {
        LinkedList<long[]> tupleList = new LinkedList<>();
        try {
            Files.lines(file.toPath()).forEach(line -> {
                String[] splitLine = line.split("\\|");
                assert (splitLine.length == attributes.size());
                long[] tuple = new long[splitLine.length];
                for (int i = 0; i < splitLine.length; ++i)
                    tuple[i] = Long.valueOf(splitLine[i]);
                tupleList.add(tuple);
            });
        } catch (IOException e) {
            throw new InternalError("Error occurred while reading in a relation", e);
        }
        tupleList.sort((tuple1, tuple2) -> {
            for (int i = 0, j = 0; i < tuple1.length && j < tuple2.length; i++, j++) {
                if (tuple1[i] != tuple2[j])
                    return Long.compare(tuple1[i], tuple2[j]);
            }
            return tuple1.length - tuple2.length;
        });
        tuples = new long[tupleList.size()][];
        for (int i = 0; tupleList.size() > 0; ++i)
            tuples[i] = tupleList.removeFirst();
    }

    /**
     * Get the name of the relation.
     * @return the name of the relation
     */
    public String getName() {
        return name;
    }

    /**
     * Get a list of attribute names in the relation.
     * @return a list of attribute names in the relation
     */
    public List<String> getAttributes() {
        return attributes;
    }

    /**
     * Get the tuples in the relation, as a two-dimensional tuple of rows then attributes
     * @return all tuples in the relation
     */
    public long[][] getTuples() {
        return tuples;
    }
}
