package org.candidate697229.structures;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BTreeTest {
    private BTree<Integer, Integer> bTree = new BTree<>();

    @Before
    public void init() {
        bTree.put(1,1);
        bTree.put(2,2);
        bTree.put(3,3);
        bTree.put(4,4);
        bTree.put(6,6);
    }

    @Test
    public void testIterator() {
        bTree.clearIterator();
        assertTrue(1 == bTree.key());
        bTree.next();
        assertTrue(2 == bTree.key());
        bTree.seek(3);
        assertTrue(3 == bTree.key());
        bTree.seek(5);
        assertTrue(6 == bTree.key());
    }
}
