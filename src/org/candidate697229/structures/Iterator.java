package org.candidate697229.structures;

public interface Iterator {
    boolean atEnd();

    long key();

    boolean isNextKeySame();

    void seek(long x1);

    void next();

    void prev();

    long[] value();
}
