package com.mogujie.jessica.util;

import java.util.Iterator;

public interface SkippableIterator<E> extends Iterator<E>
{

    public abstract void skipTo(int i);

}
