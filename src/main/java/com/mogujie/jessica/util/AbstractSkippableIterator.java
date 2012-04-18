package com.mogujie.jessica.util;

import com.google.common.collect.AbstractIterator;

public abstract class AbstractSkippableIterator<E> extends AbstractIterator<E> implements SkippableIterator<E>
{

    public abstract void skipTo(int i);

}
