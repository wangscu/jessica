package com.mogujie.storeage.bitcask;

public class BitCaskOptions
{

    public int expiry_secs = 0;
    public long max_file_size = 1024 * 1024; /* 1mb file size */
    public boolean read_write = false;
    public int open_timeout_secs = 20;

    public int expiry_time()
    {
        if (expiry_secs > 0)
            return BitCaskFile.tstamp() - expiry_secs;
        else
            return 0;
    }

    public boolean is_read_write()
    {
        return read_write;
    }

}
