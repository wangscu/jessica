package com.mogujie.storeage.bitcask;

import com.google.protobuf.ByteString;

/** Iterator interface for key+value */
public interface EntryIter<T>
{

    /**
     * @param key
     *            The entry's Key
     * @param value
     *            The entry's Value
     * @param tstamp
     *            Time stamp of the entry (lower 32-bit of Nanos at write time)
     * @param entry_pos
     *            Position of this entry in the data file
     * @param entry_size
     *            Size of the entry in the data file
     * @param acc
     *            Accumulator for fold operations
     * @return accumulator for next iteration
     * 
     * @see BitCaskFile#fold(EntryIter, Object)
     */
    T each(ByteString key, ByteString value, int tstamp, long entry_pos, int entry_size, T acc);

}
