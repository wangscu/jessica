package com.mogujie.jessica.index;

import com.mogujie.jessica.util.ArrayUtil;
import com.mogujie.jessica.util.RamUsageEstimator;

public class ParallelPostingsArray
{
    final static int BYTES_PER_POSTING = 3 * RamUsageEstimator.NUM_BYTES_INT;

    public final int size;
    public final int[] textStarts;
    public int freqProxUptos[]; // docId已经写到的位置 该位置是没有数据的 如果要读数据
    public int freqProxStarts[]; // docId开始写入的位置
    public int termFreqs[];

    ParallelPostingsArray(final int size)
    {
        this.size = size;
        textStarts = new int[size];
        freqProxUptos = new int[size];
        freqProxStarts = new int[size];
        termFreqs = new int[size];
    }

    int bytesPerPosting()
    {
        return BYTES_PER_POSTING;
    }

    ParallelPostingsArray newInstance(int size)
    {
        return new ParallelPostingsArray(size);
    }

    final ParallelPostingsArray grow()
    {
        int newSize = ArrayUtil.oversize(size + 1, bytesPerPosting());
        ParallelPostingsArray newArray = newInstance(newSize);
        copyTo(newArray, size);
        return newArray;
    }

    void copyTo(ParallelPostingsArray toArray, int numToCopy)
    {
        System.arraycopy(textStarts, 0, toArray.textStarts, 0, numToCopy);
        System.arraycopy(freqProxUptos, 0, toArray.freqProxUptos, 0, numToCopy);
        System.arraycopy(freqProxStarts, 0, toArray.freqProxStarts, 0, numToCopy);
        System.arraycopy(termFreqs, 0, toArray.termFreqs, 0, numToCopy);
    }
}
