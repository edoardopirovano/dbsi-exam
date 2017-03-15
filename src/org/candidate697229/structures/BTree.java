package org.candidate697229.structures;

import java.util.Stack;

/**
 * This class is loosely based on code obtained from http://algs4.cs.princeton.edu/home/
 * with many modifications for our use case (in particular, a lot of unnecessary code for
 * deletions was removed, and all the iteration code was added).
 */
public class BTree<Key extends Comparable<Key>, Value> {
    // max children per B-tree node = M-1
    // (must be even and greater than 2)
    private static final int M = 64;

    private Node root;       // root of the B-tree
    private int height;      // height of the B-tree

    private Node curNode;
    private int curIndex;
    private boolean atEnd = false;
    private Stack<Integer> prevIndex = new Stack<>();
    private Stack<Node> prevNode = new Stack<>();

    // helper B-tree node data type
    private static final class Node {
        private int m;                             // number of children
        private Entry[] children = new Entry[M];   // the array of children
        private int depth;

        // create a node with k children
        private Node(int depth, int k) {
            this.depth = depth;
            m = k;
        }
    }

    // internal nodes: only use key and next
    // external nodes: only use key and value
    private static class Entry {
        private Comparable key;
        private final Object val;
        private Node next;     // helper field to iterate over array entries

        Entry(Comparable key, Object val, Node next) {
            this.key = key;
            this.val = val;
            this.next = next;
        }
    }

    public BTree() {
        root = new Node(0, 0);
    }

    public void clearIterator() {
        curNode = root;
        curIndex = 0;
        prevIndex.clear();
        atEnd = false;
        traverseDownLeft();
    }

    private void traverseDownLeft() {
        while (curNode.depth > 0) {
            prevIndex.push(curIndex);
            prevNode.push(curNode);
            curNode = curNode.children[curIndex].next;
            curIndex = 0;
        }
    }

    private void traverseDownRight() {
        while (curNode.depth > 0) {
            prevIndex.push(curIndex);
            prevNode.push(curNode);
            curNode = curNode.children[curIndex].next;
            curIndex = curNode.m - 1;
        }
    }

    public boolean isNextKeySame() {
        Key previous = key();
        next();
        boolean result = (!atEnd() && key() == previous);
        prev();
        return result;
    }

    public void seek(Key key) {
        if (key == key())
            return;
        while (!prevIndex.empty() && less(curNode.children[curNode.m - 1].key, key)) {
            curIndex = prevIndex.pop();
            curNode = prevNode.pop();
        }
        while (curNode.depth > 0) {
            for (int j = curIndex; j < curNode.m; j++) {
                if (j + 1 == curNode.m || less(key, curNode.children[j + 1].key)) {
                    prevIndex.push(j);
                    prevNode.push(curNode);
                    curNode = curNode.children[j].next;
                    curIndex = 0;
                    break;
                }
            }
        }
        for (int j = curIndex; j < curNode.m; j++) {
            if (leq(key, curNode.children[j].key)) {
                curIndex = j;
                goToFirst();
                return;
            }
        }
        atEnd = true;
    }

    private void goToFirst() {
        Key key = key();
        while (eq(key, key()))
            prev();
        next();
    }

    public void next() {
        while (!prevIndex.empty() && curIndex == curNode.m - 1) {
            curIndex = prevIndex.pop();
            curNode = prevNode.pop();
        }
        if (++curIndex >= curNode.m)
            atEnd = true;
        else traverseDownLeft();
    }

    public void prev() {
        atEnd = false;
        while (!prevIndex.empty() && curIndex == 0) {
            curIndex = prevIndex.pop();
            curNode = prevNode.pop();
        }
        assert (curIndex > 0);
        --curIndex;
        traverseDownRight();
    }

    public boolean atEnd() {
        return atEnd;
    }

    public void put(Key key, Value val) {
        if (key == null) throw new IllegalArgumentException("argument key to put() is null");
        Node u = insert(root, key, val, height);
        if (u == null) return;

        // need to split root
        Node t = new Node(root.depth + 1, 2);
        t.children[0] = new Entry(root.children[0].key, null, root);
        t.children[1] = new Entry(u.children[0].key, null, u);
        root = t;
        height++;
    }

    private Node insert(Node h, Key key, Value val, int ht) {
        int j;
        Entry t = new Entry(key, val, null);

        // external node
        if (ht == 0) {
            for (j = 0; j < h.m; j++) {
                if (less(key, h.children[j].key)) break;
            }
        }

        // internal node
        else {
            for (j = 0; j < h.m; j++) {
                if ((j + 1 == h.m) || less(key, h.children[j + 1].key)) {
                    Node u = insert(h.children[j++].next, key, val, ht - 1);
                    if (u == null) return null;
                    t.key = u.children[0].key;
                    t.next = u;
                    break;
                }
            }
        }

        System.arraycopy(h.children, j, h.children, j + 1, h.m - j);
        h.children[j] = t;
        h.m++;
        if (h.m < M) return null;
        else return split(h);
    }

    private Node split(Node h) {
        Node t = new Node(h.depth, M / 2);
        h.m = M / 2;
        System.arraycopy(h.children, 2, t.children, 0, M / 2);
        return t;
    }


    @SuppressWarnings("unchecked")
    public Key key() {
        return (Key) curNode.children[curIndex].key;
    }

    @SuppressWarnings("unchecked")
    public Value value() {
        return (Value) curNode.children[curIndex].val;
    }

    @SuppressWarnings("unchecked")
    private boolean less(Comparable k1, Comparable k2) {
        return k1.compareTo(k2) < 0;
    }

    @SuppressWarnings("unchecked")
    private boolean eq(Comparable k1, Comparable k2) {
        return k1.compareTo(k2) == 0;
    }

    private boolean leq(Comparable key, Comparable key1) {
        return less(key, key1) || eq(key, key1);
    }
}