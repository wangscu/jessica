package com.mogujie.jessica.store;


import com.mogujie.jessica.index.IntBlockPool;
import com.mogujie.jessica.index.InvertedIndexer;

/**
 * SimplePostingListStore 用来存储所有term的postingList。<br/>
 * posting的docId和position都存储在一个int中，int的偏移量也存储在一个int中。<br/>
 * posting指针 = docId(24bit) + position (8bits)<br/>
 * 指针int = 层级索引(2bits)+片段索引(19-28bits)+偏移量(2-11bits)<br/>
 * 局限postion 不能大于512 对于微博系统够用了<br/>
 * 每个segment最多能有1600万的doc数据<br/>
 * 每个segment最多只能有2亿个termId<br/>
 * 最多能存储40亿个posting数据 <br/>
 * 对蘑菇街来说已经够用了 :)<br/>
 * <br/>
 * SimplePostingListStore包含4个IntBlcokPool用来存储posting<br/>
 * 一个IntBlcokPool由多个32K大小的int数据组成，并可以无限扩张<br/>
 * 对于新的termId SimplePostingListStore首先在第一层分配1<<2个int<br/>
 * 并将第一个int设置为第一个docid<br/>
 * 依次类推 分别分配 1<<2, 1<<4, 1<<7, 1<<11个int的slice给对应的term，<br/>
 * 如果分配到了第4层，以后每次都分配1<<11个int<br/>
 * 每个slice的两端分别为指向上一个slice和下一个slice的指针，便于term向前，向后遍历。<br/>
 * 向后遍历是互联网应用最希望要的 :)<br/>
 * 
 * @author xuanxi
 */
public class PostingListStore
{
    public final static int INT_BLOCK_SHIFT = 15;
    public final static int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
    public final static int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;
    public final static int INT_BLOCK_LEVEL = 4;
    public final static int[] INT_SLICE_SIZE = new int[] { 1 << 2, 1 << 4, 1 << 7, 1 << 11 };
    public final static int[] INT_SLICE_SIZE_MASK = new int[] { (1 << 2) - 1, (1 << 4) - 1, (1 << 7) - 1, (1 << 11) - 1 };
    public final static int[] INT_SLICE_SIZE_SHIFT = new int[] { 2, 4, 7, 11 };
    final public InvertedIndexer indexer;
    final public IntBlockPool[] intBlockPools;
    public int terms = 0;

    public PostingListStore(InvertedIndexer indexer)
    {
        intBlockPools = new IntBlockPool[INT_BLOCK_LEVEL];
        for (int i = 0; i < INT_BLOCK_LEVEL; i++)
        {
            intBlockPools[i] = new IntBlockPool(indexer);
        }
        this.indexer = indexer;
    }

    public int newTerm()
    {
        // 将intblock指向第一层
        IntBlockPool intBlockPool = intBlockPools[0];
        // 如果第一层的剩余存储空间已经不够再分配一个Slice块出来 就增加第一层block的大小
        if (intBlockPool.intUpto + INT_SLICE_SIZE[0] > InvertedIndexer.INT_BLOCK_SIZE)
        {
            intBlockPool.nextBuffer();
        }

        // 生成一个指针指向该块int内存
        int pointer = 0 << 30 | terms << INT_SLICE_SIZE_SHIFT[0];
        // 标记第一层的块已经被分配出去一个块 intUpto指针向前移动一位
        intBlockPool.intUpto += INT_SLICE_SIZE[0];
        // term数据加一
        terms += 1;
        return pointer;
    }

    public void reset()
    {
        for (int i = 0; i < 4; i++)
        {
            IntBlockPool intBlockPool = intBlockPools[i];
            intBlockPool.reset();
        }
    }

}
