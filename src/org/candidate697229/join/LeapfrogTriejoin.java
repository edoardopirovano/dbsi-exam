package org.candidate697229.join;

import org.candidate697229.structures.Iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of a Leapfrog Triejoin. Based on:
 *      Leapfrog Triejoin: A Simple, Worst-Case Optimal Join Algorithm by Todd L. Veldhuizen
 *      https://openproceedings.org/2014/conf/icdt/Veldhuizen14.pdf
 *
 * Uses one UnaryLeapfrogTriejoin for each join condition, as described in the paper.
 * Provides methods for iterating over the overall join result.
 */
public class LeapfrogTriejoin {
    private final UnaryLeapfrogTriejoin[] unaryLeapfrogTriejoins;
    private boolean overallAtEnd = false;
    private int depth = -1;

    /**
     * Construct a new instance of the join algorithm.
     *
     * @param iterators      the iterators for the data we wish to join on
     * @param joinConditions the conditions for the join, these should be a list of list of pairs, with each pair
     *                       representing a table and attribute within that table that we wish to be equal to all
     *                       others in the same list
     */
    public LeapfrogTriejoin(Iterator[] iterators, List<List<int[]>> joinConditions) {
        unaryLeapfrogTriejoins = joinConditions.stream().map(joinInstruction -> {
                List<Iterator> usedIterators = new ArrayList<>(joinInstruction.size());
                usedIterators.addAll(joinInstruction.stream().map(position -> iterators[position[0]]).collect(Collectors.toList()));
                return new UnaryLeapfrogTriejoin(usedIterators);
            }).collect(Collectors.toList()).toArray(new UnaryLeapfrogTriejoin[0]);
        findNext(false);
    }

    /**
     * Check if we have reached the end of the results of the join.
     * @return true if there are no more results to the join
     */
    public boolean overallAtEnd() {
        return overallAtEnd;
    }

    /**
     * Advance the join along by one. Notice this will not advance within blocks of rows that have equal join attributes,
     * but rather advance to the next block. It is up to the caller to use the iterators to access the block with equal
     * join attributes if desired.
     */
    public void overallNext() {
        findNext(true);
    }

    /**
     * Find the next value in the overall join by performing a backtracking search in the binding tree.
     * @param shouldAdvance true to advance from the current value, false to simply find the first value
     */
    private void findNext(boolean shouldAdvance) {
        do {
            while (depth > 0 && atEnd()) {
                up();
                next();
                if (!atEnd())
                    shouldAdvance = false;
            }
            if (depth == 0 && atEnd()) {
                overallAtEnd = true;
                return;
            }
            if (shouldAdvance) {
                next();
                shouldAdvance = atEnd();
            }
            while (depth < unaryLeapfrogTriejoins.length - 1) {
                open();
                if (atEnd())
                    break;
            }
        } while (atEnd());
    }

    /**
     * Check if the iterator at the current depth is at the end.
     * @return whether or not the iterator at the current depth is at the end
     */
    private boolean atEnd() {
        return unaryLeapfrogTriejoins[depth].atEnd();
    }

    /**
     * Advance the iterator at the current depth.
     */
    private void next() {
        unaryLeapfrogTriejoins[depth].next();
    }

    /**
     * Open (move down and to the leftmost value) the iterator at the current depth.
     */
    private void open() {
        unaryLeapfrogTriejoins[++depth].open();
    }

    /**
     * Move up the iterator at the current depth.
     */
    private void up() {
        unaryLeapfrogTriejoins[depth--].up();
    }
}

