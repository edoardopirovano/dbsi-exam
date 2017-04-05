package org.candidate697229.structures;

public interface Iterator {
    boolean atEnd();

    long key();

    void seek(long x1);

    void next();

    long[] value();

    void open();

    void up();

    void nextInBlock();

    boolean isNextInBlock();

    void back(int numOfValues);
}
