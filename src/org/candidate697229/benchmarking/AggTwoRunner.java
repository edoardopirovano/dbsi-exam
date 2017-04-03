package org.candidate697229.benchmarking;

import org.candidate697229.algorithms.AggTwo;
import org.candidate697229.database.Database;

import java.util.Arrays;

class AggTwoRunner implements QueryRunner {
    private boolean debug = true;

    @Override
    public long runQueryAll(int database) {
        long start = System.currentTimeMillis();
        long[] result = new AggTwo(Database.makeFromDirectory("housing/housing-" + database)).computeAllAggregatesOfNaturalJoin();
        if (debug)
            System.out.println("RESULT\tAggTwoRunner\t" + database + "\tAll\t" + Arrays.toString(result));
        return System.currentTimeMillis() - start;
    }

    @Override
    public long runQueryOne(int database) {
        long start = System.currentTimeMillis();
        long result = new AggTwo(Database.makeFromDirectory("housing/housing-" + database)).computeOneAggregateOfNaturalJoin();
        if (debug)
            System.out.println("RESULT\tAggTwoRunner\t" + database + "\tOne\t[" + result + "]");
        return System.currentTimeMillis() - start;
    }
}
