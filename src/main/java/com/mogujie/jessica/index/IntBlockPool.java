package com.mogujie.jessica.index;

import java.util.Arrays;

public final class IntBlockPool extends IntBlocks
{
    public int bufferUpto = -1;
    public int intUpto = InvertedIndexer.INT_BLOCK_SIZE;
    public int[] buffer;
    public int intOffset = -InvertedIndexer.INT_BLOCK_SIZE;

    final private InvertedIndexer indexer;

    public IntBlockPool(InvertedIndexer indexer)
    {
        super(new int[10][]);
        this.indexer = indexer;
    }

    public void reset()
    {
        if (bufferUpto != -1)
        {
            if (bufferUpto > 0)
            {
                indexer.recycleIntBlocks(buffers, 1, bufferUpto - 1);
                Arrays.fill(buffers, 1, bufferUpto, null);
            }
            bufferUpto = 0;
            intUpto = 0;
            intOffset = 0;
            buffer = buffers[0];
        }
    }

    public void nextBuffer()
    {
        if (1 + bufferUpto == buffers.length)
        {
            int[][] newBuffers = new int[(int) (buffers.length * 1.5)][];
            System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
            buffers = newBuffers;
        }
        buffer = buffers[1 + bufferUpto] = indexer.getIntBlock();
        bufferUpto++;

        intUpto = 0;
        intOffset += InvertedIndexer.INT_BLOCK_SIZE;
    }
}
