package com.mogujie.jessica.query;

import com.google.common.base.Predicate;

public interface QueryMatcher
{

    public SimpleUids getAllMatches(Query query, int limit);

    public int countMatches(Query query) throws InterruptedException;

    public int countMatches(Query query, Predicate<Integer> idFilter) throws InterruptedException;

    public boolean hasChanges(Integer docid) throws InterruptedException;

}
