package org.candidate697229.join;

import org.candidate697229.structures.Iterator;

import java.util.List;

/**
 * Implementation of a unary Leapfrog Triejoin. Based on:
 * Leapfrog Triejoin: A Simple, Worst-Case Optimal Join Algorithm by Todd L. Veldhuizen
 * https://openproceedings.org/2014/conf/icdt/Veldhuizen14.pdf
 * The code given here is an exact implementation of the pseudocode in the paper.
 */
class UnaryLeapfrogTriejoin {
    private final Iterator[] iterators;
    private boolean atEnd;
    private int p = 0;

    /**
     * Construct a unary Leapfrog Triejoin.
     * @param iterators the data iterators to use
     */
    UnaryLeapfrogTriejoin(List<Iterator> iterators) {
        this.iterators = iterators.toArray(new Iterator[0]);
    }

    /**
     * Perform initialization. This will set atEnd to true if any iterator is at the end and otherwise
     * perform a leapfrog search to attempt to find the first join result.
     */
    private void init() {
        atEnd = false;
        for (Iterator iterator : iterators) {
            if (iterator.atEnd())
                atEnd = true;
        }
        if (!atEnd)
            leapfrogSearch();
    }

    /**
     * Perform a leapfrog search, which will either advance the iterators to the next join result, or set atEnd to true
     * if no such join result can be found.
     */
    private void leapfrogSearch() {
        long x1 = iterators[Math.floorMod(p - 1, iterators.length)].key();
        while (true) {
            long x = iterators[p].key();
            if (x == x1) {
                return;
            } else {
                iterators[p].seek(x1);
                if (iterators[p].atEnd()) {
                    atEnd = true;
                    return;
                } else {
                    x1 = iterators[p].key();
                    p = Math.floorMod(p + 1, iterators.length);
                }
            }
        }
    }

    /**
     * Advance to the next join result.
     */
    void next() {
        iterators[p].next();
        if (iterators[p].atEnd())
            atEnd = true;
        else {
            p = Math.floorMod(p + 1, iterators.length);
            leapfrogSearch();
        }
    }

    /**
     * Check if we have reached the end of the join results.
     * @return true if we have reached the end of the join results
     */
    boolean atEnd() {
        return atEnd;
    }

    /**
     * Open this join by opening each of the iterators it uses and then initializing it.
     */
    void open() {
        for (Iterator iterator : iterators) iterator.open();
        init();
    }

    /**
     * Move up every iterator used by this join.
     */
    void up() {
        for (Iterator iterator : iterators) iterator.up();
    }
}
