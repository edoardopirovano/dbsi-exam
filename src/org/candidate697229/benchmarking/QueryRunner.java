package org.candidate697229.benchmarking;

import org.candidate697229.algorithms.AggAlgorithm;

import java.util.Arrays;

import static org.candidate697229.config.Configuration.OUTPUT_RESULTS;

/**
 * Abstract class describing a class that can run and time queries.
 */
abstract class QueryRunner {
    /**
     * Run a query computing the sum of each product of attribute pairs on a database.
     * @param database the scale factor of the database to run on
     * @return the time to run the query
     */
    long runQueryAll(int database) {
        long start = System.currentTimeMillis();
        AggAlgorithm algorithm = getAlgorithm(database);
        long[] result = algorithm.computeAllAggregatesOfNaturalJoin();
        if (OUTPUT_RESULTS)
            System.out.println("RESULT\t" + getClass().getSimpleName() + "\t"
                    + database + "\tAll\t" + Arrays.toString(result));
        return System.currentTimeMillis() - start;
    }

    /**
     * Run a query computing the sum of one product of attribute pairs on a database.
     * @param database the scale factor of the database to run on
     * @return the time to run the query
     */
    long runQueryOne(int database) {
        long start = System.currentTimeMillis();
        AggAlgorithm algorithm = getAlgorithm(database);
        long result = algorithm.computeOneAggregateOfNaturalJoin();
        if (OUTPUT_RESULTS)
            System.out.println("RESULT\t" + getClass().getSimpleName() + "\t"
                    + database + "\tOne\t" + result);
        return System.currentTimeMillis() - start;
    }

    abstract AggAlgorithm getAlgorithm(int database);
}
