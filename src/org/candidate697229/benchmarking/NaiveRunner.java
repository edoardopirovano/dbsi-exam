package org.candidate697229.benchmarking;

import org.candidate697229.algorithms.AggAlgorithm;
import org.candidate697229.algorithms.Naive;

/**
 * Class to run queries using a Naive SQLite database.
 */
class NaiveRunner extends QueryRunner {
    @Override
    AggAlgorithm getAlgorithm(int database) {
        return new Naive(database);
    }
}
