package com.mogujie.jessica.util;

import com.google.common.collect.Iterables;

/**
 * Provides an implementation of {@code Object#toString} for {@code Iterable}
 * instances.
 * 
 * @author Mike Bostock
 */
public abstract class AbstractIterable<E> implements Iterable<E>
{
    /**
     * Returns a string representation of this iterable. The string
     * representation consists of a list of the iterable's elements in the order
     * they are returned by its iterator, enclosed in square brackets ("[]").
     * Adjacent elements are separated by the characters ", " (comma and space).
     * Elements are converted to strings as by {@link String#valueOf(Object)}.
     */
    @Override
    public String toString()
    {
        return Iterables.toString(this);
    }
}
