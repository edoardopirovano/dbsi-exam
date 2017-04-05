package org.candidate697229.algorithms;

/**
 * Interface for an implementation of our aggregation calculation.
 */
public interface AggAlgorithm {
    /**
     * Run a query computing the sum of each product of attribute pairs in the database.
     * @return the result of the query
     */
    long[] computeAllAggregatesOfNaturalJoin();

    /**
     * Run a query computing the sum of one product of attribute pairs in the database.
     * @return the result of the query
     */
    long computeOneAggregateOfNaturalJoin();
}
