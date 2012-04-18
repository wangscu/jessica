package com.mogujie.jessica.util;

import java.util.Iterator;

public abstract class TerminalbleSkippableIterator<E> implements SkippableIterator<E>, Iterator<E>
{
    // 是否已经中断
    private volatile boolean isTerminal = false;

    public boolean isTerminal()
    {
        return isTerminal;
    }

    public void terminal()
    {
        this.isTerminal = true;
    }

}