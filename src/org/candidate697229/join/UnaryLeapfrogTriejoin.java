package org.candidate697229.join;

import org.candidate697229.structures.Iterator;

import java.util.List;

class UnaryLeapfrogTriejoin {
    private final Iterator[] iterators;
    private int k;

    UnaryLeapfrogTriejoin(List<Iterator> iterators) {
        this.iterators = iterators.toArray(new Iterator[0]);
        k = iterators.size();
    }

    private boolean atEnd;
    private int p = 0;

    private void init() {
        atEnd = false;
        for (int i = 0; i < k; ++i) {
            if (iterators[i].atEnd())
                atEnd = true;
        }
        if (!atEnd)
            leapfrogSearch();
    }

    private void leapfrogSearch() {
        long x1 = iterators[Math.floorMod(p - 1, k)].key();
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
                    p = Math.floorMod(p + 1, k);
                }
            }
        }
    }

    void next() {
        iterators[p].next();
        if (iterators[p].atEnd())
            atEnd = true;
        else {
            p = Math.floorMod(p + 1, k);
            leapfrogSearch();
        }
    }

    boolean atEnd() {
        return atEnd;
    }

    void open() {
        for (Iterator iterator : iterators) iterator.open();
        init();
    }

    void up() {
        for (Iterator iterator : iterators) iterator.up();
    }
}
