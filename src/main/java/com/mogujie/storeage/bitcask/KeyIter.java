package com.mogujie.storeage.bitcask;

import com.google.protobuf.ByteString;

/** Iterator for folding over keys */
public interface KeyIter<T>
{

    /**
     * @param key
     *            The entry's Key
     * @param tstamp
     *            Time stamp of the entry (lower 32-bit of Nanos at write time)
     * @param entry_pos
     *            Position of this entry in the data file
     * @param entry_size
     *            Size of the entry in the data file
     * @param acc
     *            Accumulator for fold operations
     * @return accumulator for next iteration
     * @throws Exception
     * 
     * @see BitCaskFile#fold_keys(KeyIter, Object)
     */

    T each(ByteString key, int tstamp, long entry_pos, int entry_size, T acc) throws Exception;

}
