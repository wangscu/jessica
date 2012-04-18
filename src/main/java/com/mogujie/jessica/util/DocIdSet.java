package com.mogujie.jessica.util;

import java.io.IOException;

/**
 * A DocIdSet contains a set of doc ids. Implementing classes must only
 * implement {@link #iterator} to provide access to the set.
 */
public abstract class DocIdSet
{

    /**
     * An empty {@code DocIdSet} instance for easy use, e.g. in Filters that hit
     * no documents.
     */
    public static final DocIdSet EMPTY_DOCIDSET = new DocIdSet()
    {

        private final DocIdSetIterator iterator = new DocIdSetIterator()
        {
            @Override
            public int advance(int target) throws IOException
            {
                return NO_MORE_DOCS;
            }

            @Override
            public int docID()
            {
                return NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() throws IOException
            {
                return NO_MORE_DOCS;
            }
        };

        @Override
        public DocIdSetIterator iterator()
        {
            return iterator;
        }

        @Override
        public boolean isCacheable()
        {
            return true;
        }
    };

    /**
     * Provides a {@link DocIdSetIterator} to access the set. This
     * implementation can return <code>null</code> or
     * <code>{@linkplain #EMPTY_DOCIDSET}.iterator()</code> if there are no docs
     * that match.
     */
    public abstract DocIdSetIterator iterator() throws IOException;

    /**
     * This method is a hint for {@link CachingWrapperFilter}, if this
     * <code>DocIdSet</code> should be cached without copying it into a BitSet.
     * The default is to return <code>false</code>. If you have an own
     * <code>DocIdSet</code> implementation that does its iteration very
     * effective and fast without doing disk I/O, override this method and
     * return <code>true</here>.
     */
    public boolean isCacheable()
    {
        return false;
    }
}
