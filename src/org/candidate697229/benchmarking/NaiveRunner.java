package org.candidate697229.benchmarking;

import org.candidate697229.algorithms.Naive;
import org.candidate697229.database.Database;

import java.util.List;
import java.util.stream.Collectors;

import static org.candidate697229.config.Configuration.USE_TEST_TABLE;

class NaiveRunner implements QueryRunner {
    final private String queryAll;
    final private String queryOne;
    private boolean debug = true;

    NaiveRunner() {
        Database sampleDatabase = Database.makeFromDirectory(null, false);
        queryAll = Naive.buildQueryAll(sampleDatabase);
        queryOne = Naive.buildQueryOne(sampleDatabase);
    }

    @Override
    public long runQueryAll(int database) {
        return timeQuery(database, queryAll, "All");
    }

    @Override
    public long runQueryOne(int database) {
        return timeQuery(database, queryOne, "One");
    }

    private long timeQuery(int database, String query, String name) {
        long start = System.currentTimeMillis();
        List<Long> result = Naive.runQuery(USE_TEST_TABLE ? "test-table.db" : "housing/housing-" + database + ".db", query);
        if (debug)
            System.out.println("RESULT\tNaiveRunner\t" + database + "\t" + name + "\t[" +
                    result.stream().map(Object::toString)
                    .collect(Collectors.joining(", ")) + "]");
        return System.currentTimeMillis() - start;
    }
}
