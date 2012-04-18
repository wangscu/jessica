package com.mogujie.storeage.bitcask;

public class BitCaskEntry
{

    public final int tstamp;
    public int file_id;
    public final long offset;
    public final int total_sz;

    public BitCaskEntry(int file_id, int ts, long offset, int total_sz)
    {
        this.file_id = file_id;
        this.tstamp = ts;
        this.offset = offset;
        this.total_sz = total_sz;
    }

    boolean is_newer_than(BitCaskEntry old)
    {
        return old.tstamp < tstamp || (old.tstamp == tstamp && (old.file_id < file_id || (old.file_id == file_id && old.offset < offset)));
    }
}
