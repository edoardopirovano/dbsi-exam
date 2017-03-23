package org.candidate697229.database;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;

public class Table {
    private String name;
    private List<String> attributes;
    private long[][] tuples;

    public Table(String name, List<String> attributes) {
        this.name = name;
        this.attributes = attributes;
    }

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
            throw new InternalError(e);
        }
        tuples = new long[tupleList.size()][];
        for (int i = 0; tupleList.size() > 0; ++i)
            tuples[i] = tupleList.removeFirst();
    }

    public String getName() {
        return name;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public long[][] getTuples() {
        return tuples;
    }

    public void putTuples(long[][] tuples) {
        this.tuples = tuples;
    }
}
