package com.mogujie.storeage.bitcask;

import com.google.protobuf.ByteString;

/** Iterator interface for key+value */
public interface KeyValueIter<T>
{

    /**
     * @param key
     *            The entry's Key
     * @param value
     *            The entry's Value
     * @param acc
     *            Accumulator for fold operations
     * @return accumulator for next iteration
     * 
     * @see BitCaskFile#fold(KeyValueIter, Object)
     */
    T each(ByteString key, ByteString value, T acc);

}
