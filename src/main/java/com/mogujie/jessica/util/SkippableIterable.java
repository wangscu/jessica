package com.mogujie.jessica.util;

public interface SkippableIterable<E> extends Iterable<E>
{

    public abstract SkippableIterator<E> iterator();

}
