package org.candidate697229.benchmarking;

import org.candidate697229.algorithms.AggOne;
import org.candidate697229.database.Database;

import java.util.List;

class AggOneRunner implements QueryRunner {
    private final List<Database> databases;

    AggOneRunner(List<Database> databases) {
        this.databases = databases;
    }

    @Override
    public long runQueryAll(int database) {
        long start = System.currentTimeMillis();
        new AggOne(databases.get(database)).computeAllAggregatesOfNaturalJoin();
        return System.currentTimeMillis() - start;
    }

    @Override
    public long runQueryOne(int database) {
        long start = System.currentTimeMillis();
        new AggOne(databases.get(database)).computeOneAggregateOfNaturalJoin();
        return System.currentTimeMillis() - start;
    }
}
