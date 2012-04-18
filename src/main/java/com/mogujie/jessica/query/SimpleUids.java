package com.mogujie.jessica.query;

import java.io.Serializable;

/**
 * 一个没有打分的Id序列
 * 
 * @author xuanxi
 * 
 */
public class SimpleUids implements Serializable
{
    private static final long serialVersionUID = 1L;
    private long uids[];
    private int size;
    private int capacity;

    public SimpleUids()
    {
        capacity = 1024;
        uids = new long[capacity];
        size = 0;
    }

    public void add(long docId)
    {
        uids[size++] = docId;
        if (size >= capacity)
        {
            grow();
        }
    }

    public int size()
    {
        return size;
    }

    public long[] uids()
    {
        long[] temp = new long[size];
        System.arraycopy(uids, 0, temp, 0, size);
        return temp;
    }

    public void grow()
    {
        capacity = capacity * 2;
        long[] newUids = new long[capacity];
        System.arraycopy(uids, 0, newUids, 0, capacity / 2);
        uids = newUids;
    }
}
