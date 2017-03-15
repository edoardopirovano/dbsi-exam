package org.candidate697229.benchmarking;

import org.candidate697229.algorithms.Naive;
import org.candidate697229.database.Database;

class NaiveRunner implements QueryRunner {
    final private String queryAll;
    final private String queryOne;

    NaiveRunner() {
        Database sampleDatabase = Database.makeFromDirectory(null, false);
        queryAll = Naive.buildQueryAll(sampleDatabase);
        queryOne = Naive.buildQueryOne(sampleDatabase);
    }

    @Override
    public long runQueryAll(int database) {
        return timeQuery("housing/housing-" + database + ".db", queryAll);
    }

    @Override
    public long runQueryOne(int database) {
        return timeQuery("housing/housing-" + database + ".db", queryOne);
    }

    private long timeQuery(String database, String query) {
        long start = System.currentTimeMillis();
        Naive.runQuery(database, query);
        return System.currentTimeMillis() - start;
    }
}
