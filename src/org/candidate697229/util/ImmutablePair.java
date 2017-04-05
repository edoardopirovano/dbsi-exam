package org.candidate697229.util;

/**
 * Class representing an immutable pair of objects.
 * @param <S> the class of the first object in the pair
 * @param <T> the class of the second object it the pair
 */
public class ImmutablePair<S, T> {
    private final S first;
    private final T second;

    /**
     * Build a pair consisting of two objects.
     * @param first the first object
     * @param second the second object
     */
    public ImmutablePair(S first, T second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Get the first object in this pair.
     * @return the first object in the pair
     */
    public S getFirst() {
        return first;
    }

    /**
     * Get the second object in this pair.
     * @return the second object in the pair
     */
    public T getSecond() {
        return second;
    }
}
