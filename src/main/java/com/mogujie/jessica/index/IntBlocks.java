package com.mogujie.jessica.index;

public class IntBlocks
{
    public int[][] buffers;

    public IntBlocks(int[][] buffers)
    {
        this.buffers = buffers;
    }

    public IntBlocks copyRef()
    {
        return new IntBlocks(buffers);
    }
}
