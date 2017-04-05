package org.candidate697229.benchmarking;

import org.candidate697229.algorithms.AggAlgorithm;
import org.candidate697229.algorithms.AggTwo;

/**
 * Class to run queries with both improvements.
 */
class AggTwoRunner extends QueryRunner {
    @Override
    AggAlgorithm getAlgorithm(int database) {
        return new AggTwo(database);
    }
}
