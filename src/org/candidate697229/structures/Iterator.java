package org.candidate697229.structures;

/**
 * Interface for iterating over tuples in a database.
 */
public interface Iterator {
    /**
     * Check if there are more tuples with a different value in position depth and the same
     * values in all of the preceding positions.
     * @return true if we have reached the end of the data we are iterating
     */
    boolean atEnd();

    /**
     * Get the current key.
     * @return the key of the current data element (ie. the value
     */
    long key();

    /**
     * Move to the first tuple with a key greater than or equal to a certain value.
     * @param x the value to look for
     */
    void seek(long x);

    /**
     * Advance to the next tuple with a strictly larger value in position depth.
     */
    void next();

    /**
     * Get the current value of the iterator.
     * @return the tuple at the current position
     */
    long[] value();

    /**
     * Increment the depth and go back to the first tuple at the next depth.
     */
    void open();

    /**
     * Decrement the depth.
     */
    void up();

    /**
     * Move to the next tuple with the same key value. Notice this is different from next() in this will keep the first
     * depth values of the tuple constant, whereas next() will continue until the key changes.
     */
    void nextInBlock();

    /**
     * Check if the next tuple is in the same block.
     * @return whether or not the first depth values of the tuple are the same
     */
    boolean isNextInBlock();

    /**
     * Go back a certain number of tuples.
     * @param numOfValues how many tuples to go back by
     */
    void back(int numOfValues);
}
