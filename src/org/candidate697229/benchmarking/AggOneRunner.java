package org.candidate697229.benchmarking;

import org.candidate697229.algorithms.AggAlgorithm;
import org.candidate697229.algorithms.AggOne;

/**
 * Class to run queries with the first improvement.
 */
class AggOneRunner extends QueryRunner {
    @Override
    AggAlgorithm getAlgorithm(int database) {
        return new AggOne(database);
    }
}
