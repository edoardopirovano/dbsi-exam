package org.candidate697229.join;

import org.candidate697229.database.Database;
import org.candidate697229.structures.Iterator;
import org.candidate697229.structures.SequentialIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of a Leapfrog Triejoin
 */
public class LeapfrogTriejoin {
    private final Iterator[] iterators;
    private final UnaryLeapfrogTriejoin[] unaryLeapfrogTriejoins;
    private int k;
    private long[][] resultTuple;
    private boolean overallAtEnd = false;
    private int depth = -1;

    public LeapfrogTriejoin(Database database, List<List<int[]>> joinInstructions) {
        k = database.getRelations().size();
        iterators = new Iterator[k];
        for (int i = 0; i < k; ++i)
            iterators[i] = new SequentialIterator(database.getRelations().get(i).getTuples());
        resultTuple = new long[k][];
        unaryLeapfrogTriejoins = joinInstructions.stream().map(joinInstruction -> {
                List<Iterator> usedIterators = new ArrayList<>(joinInstruction.size());
                usedIterators.addAll(joinInstruction.stream().map(position -> iterators[position[0]]).collect(Collectors.toList()));
                return new UnaryLeapfrogTriejoin(usedIterators);
            }).collect(Collectors.toList()).toArray(new UnaryLeapfrogTriejoin[0]);
    }

    public Iterator[] getIterators() {
        return iterators;
    }

    public long[][] resultTuple() {
        for (int i = 0; i < k; ++i)
            resultTuple[i] = iterators[i].value();
        return resultTuple;
    }

    public void init() {
        findNext(false);
    }

    public boolean overallAtEnd() {
        return overallAtEnd;
    }

    public void overallNext() {
        findNext(true);
    }

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

    private boolean atEnd() {
        return unaryLeapfrogTriejoins[depth].atEnd();
    }

    private void next() {
        unaryLeapfrogTriejoins[depth].next();
    }

    private void open() {
        unaryLeapfrogTriejoins[++depth].open();
    }

    private void up() {
        unaryLeapfrogTriejoins[depth--].up();
    }
}

