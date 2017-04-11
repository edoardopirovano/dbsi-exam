package org.candidate697229.util;

/**
 * Class containing various constants used to configure the execution of the program.
 */
public class Configuration {
    /**
     * Set to true to use the test database, which consists of the example relations from the question paper with the
     * test data used as an example in question one. Set to false to use the housing databases from the course web page.
     */
    public static final boolean USE_TEST_DATABASE = false;

    /**
     * Set to true to output the query results for each run rather than just the timing results.
     */
    public static final boolean OUTPUT_RESULTS = false;

    /**
     * Number of scale factors to run timing experiments on (20 will run all of them). This is ignored if we are using
     * the test database (ie. if USE_TEST_DATABASE is true).
     */
    public static final int NUM_OF_SCALES = 20;

    /**
     * How many seconds to timeout an experiment after. This is a total number of seconds we have been testing a given
     * implementation for, and a timeout will make the benchmarker move on to the next implementation as soon as the
     * current query has finished executing.
     */
    public static final int TIMEOUT_SECONDS = 900; // 15 minutes

    /**
     * Number of times to time each query for each scale factor. Notice this does not include the first time, which is
     * not used in the timing.
     */
    public static final int REPEATS_PER_SCALE = 4;

    /**
     * Private constructor to ensure class cannot be accidentally instantiated (it is intended only to use static variables).
     */
    private Configuration() {
    }
}
