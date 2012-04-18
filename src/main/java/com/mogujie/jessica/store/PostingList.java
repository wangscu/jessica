package com.mogujie.jessica.store;

import org.apache.log4j.Logger;

import com.mogujie.jessica.index.IntBlockPool;
import com.mogujie.jessica.index.InvertedIndexer;
import com.mogujie.jessica.index.ParallelPostingsArray;
import com.mogujie.jessica.util.Bits;

public class PostingList
{
    private static final Logger logger = Logger.getLogger(PostingList.class);
    public static final int NO_MORE_DOCS = Integer.MAX_VALUE;

    int numDocs = 0;
    int docID = 0;
    int docFreq = 0;
    final int termID;
    // TODO 不管任何操作 只要target大于macDocId 返回NO_MORE_DATA
    final int maxDocID;
    // 当前位置信息集合
    int[] position;
    // 表示当前位置的下标 该值不能大于或者等于docFreq的值
    int positionIndex = 0;
    int startFreqProxPointer = 0;
    int freqProxPointer = 0;// 向new往old找docId的指针
    // TODO 如果一个文档已经标记删除 就不能参与搜索 标记一个docId是否还是有效的 具体实现是采用了一个16m的int数组保存
    final Bits bits;
    final ParallelPostingsArray postingsArray;
    final PostingListStore plStore;

    // 一个额外的skipList数据结构 方便查询
    int[] skipListDocIds = new int[4];
    int[] skipListPointers = new int[4];
    int skipListIdx = 0;

    public PostingList(int termID, int maxDocID, ParallelPostingsArray postingsArray, PostingListStore plStore, Bits bits)
    {
        this.termID = termID;
        this.maxDocID = maxDocID;
        this.postingsArray = postingsArray;
        this.bits = bits;
        this.startFreqProxPointer = postingsArray.freqProxStarts[termID];
        this.freqProxPointer = postingsArray.freqProxUptos[termID];
        this.plStore = plStore;
        init();
    }

    /**
     * skip到不大于maxDocId的位置<br/>
     * TODO 以下步骤应该在posting初始化的时候进行处理<br/>
     * 如果当前头指针指向的数据为空或者不存在 说明存储数据还没有跳过的内存障碍<br/>
     * 这个时候就要用末尾指针遍历出来确定已经越过内存障碍的头指针是多少<br/>
     */
    private void init()
    {
        // TODO 由于系统在写入FreqProx信息的时候 最后一个指针的位置处是用来写下一个数据的
        // 所以头指针要回退1位
        Pointer p = new Pointer(this.freqProxPointer);
        p = new Pointer(p.poolIdx, p.sliceIdx, p.offsetIdx - 1);
        // FIXME 考虑越界
        this.freqProxPointer = p.pointer;

        // 获得了pointer 检查该位置处的数据是否已经安全发布 如果没有安全发布
        IntBlockPool intBlockPool = plStore.intBlockPools[p.poolIdx];
        int intUptoStart = p.sliceIdx * PostingListStore.INT_SLICE_SIZE[p.poolIdx] + p.offsetIdx;
        boolean needBackForward = false;
        // bufferIdx
        int bufferIdx = intUptoStart >>> InvertedIndexer.INT_BLOCK_SHIFT;
        // 1,如果整个block都没有越过内存屏障
        if (intBlockPool.bufferUpto < bufferIdx)
        {
            needBackForward = true;
        } else
        {
            int[] buffer = intBlockPool.buffers[bufferIdx];
            int startIdx = intUptoStart & InvertedIndexer.INT_BLOCK_MASK;
            int pointer = buffer[startIdx];
            if (pointer == 0)
            {// 该指针位置的docId没有越过内存屏障
                needBackForward = true;
            }
        }

        // 则通过尾指针向后遍历 找到最新的maxDocId
        // 从尾指针开始找到maxDocId
        if (needBackForward)
        {
            logger.warn("the max docId of this term does not cross the memory barries, so let's begin forward to the maxDocId start with freqProxStarts!");
            freqProxPointer = postingsArray.freqProxStarts[termID];
            forward(maxDocID);
        } else
        { // 可能当前的intBlock数据已经越过了内存屏障 为了保证数据的一致性 所以要跳到macDocId出
            advance(maxDocID);
        }
    }

    /**
     * 该方法只做位置定位 不做freq计算 和位置数据的计算 如由此要求请调用nextDoc一次<br/>
     * 从当前位置找到离目标target docId的位置最近的最小一个文档Id 并将nextFreqProxPointer指向该位置处<br/>
     * step 1: 通过跳表的方式 查找到第一个首位freqProx数据中的docId小于等于预期target docId的Slice
     * 来确定target docId 存在的Slice<br/>
     * step 2:
     * 在第一步确定的Slice采用二分查找lowPointer<->nextFreqProxPointer中间第一个文档大于等于target docId
     * 作为当前nextFreqProxPointer<br/>
     * step 3: 顺序往回查找第一个docId小于等于target docId位置作为正在的nextFreqProxPointer<br/>
     * 
     * @param target
     *            目标预期的文档Id
     * @return docId 里目标Id最近的最小Id
     */
    public int forward(int target)
    {
        Pointer p;
        // 当前块池子的索引
        int intUptoStart;
        // 当前块在块池子中的索引
        int bufferIdx;
        // 当前块池子
        IntBlockPool intBlockPool;
        // 当前buffer块
        int[] buffer;
        // 当前数据的buffer位置
        int startIdx;
        // 当前存储数据
        int freqProx;
        int pointer;
        // 1,Skip Search
        while (true)
        {
            p = new Pointer(freqProxPointer);
            intBlockPool = plStore.intBlockPools[p.poolIdx];
            // 遍历到该Slice的最后一个指针 如果该位置没有数据就跳出循环
            int lastIdx = PostingListStore.INT_SLICE_SIZE[p.poolIdx] - 2;
            intUptoStart = p.sliceIdx * PostingListStore.INT_SLICE_SIZE[p.poolIdx] + lastIdx;
            bufferIdx = intUptoStart >>> InvertedIndexer.INT_BLOCK_SHIFT;
            buffer = intBlockPool.buffers[bufferIdx];
            startIdx = intUptoStart & InvertedIndexer.INT_BLOCK_MASK;
            freqProx = buffer[startIdx];
            if (freqProx == 0)
            { // so no data write in here
                freqProxPointer = (new Pointer(p.poolIdx, p.sliceIdx, lastIdx)).pointer;
                break;
            }

            docID = freqProx >>> 8;
            pointer = buffer[startIdx + 1];
            if (pointer == 0)
            {// so this is the newest slice in this term
                freqProxPointer = (new Pointer(p.poolIdx, p.sliceIdx, lastIdx)).pointer;
            } else if (docID >= target)
            {// the target docId must in this slice
                freqProxPointer = (new Pointer(p.poolIdx, p.sliceIdx, lastIdx)).pointer;
                break;
            } else
            { // go to the newer slice of this term
                freqProxPointer = pointer;
            }
        }

        // 找到了 最后一个slice 再看看该slice数据已经写到哪儿了
        Pointer lp = p;
        Pointer hp = new Pointer(freqProxPointer);
        // 当前slice在当前buffer的开始位置
        int sliceUptoStart = lp.sliceIdx * PostingListStore.INT_SLICE_SIZE[lp.poolIdx];
        int offsetIdx;
        // 2, Binary Search
        while (true)
        {
            // 分到中间的位置
            offsetIdx = (lp.offsetIdx + hp.offsetIdx) / 2;
            if (lp.offsetIdx == offsetIdx || hp.offsetIdx == offsetIdx)
            {// 没有找到和target相同的docID 但是已经搜索完成了 就找最接近的了
                offsetIdx = lp.offsetIdx;
                break;
            }
            // 找到索引位置
            startIdx = (sliceUptoStart + offsetIdx) & InvertedIndexer.INT_BLOCK_SIZE;
            freqProx = buffer[startIdx];
            // 解码当前的docId信息 如果为0 或者大于maxDocId 都要再查找
            docID = freqProx >>> 8;
            if (freqProx == 0)
            {
                hp = new Pointer(lp.poolIdx, lp.sliceIdx, offsetIdx);
            } else if (docID > target)
            {// 如果数据为空 就二分到下面查找
                hp = new Pointer(lp.poolIdx, lp.sliceIdx, offsetIdx);
            } else if (docID < target)
            {
                lp = new Pointer(lp.poolIdx, lp.sliceIdx, offsetIdx);
            } else
            {// 找到了个不为空的数据 再向前找 找到最后一个不为空的数据
                break;
            }
        }

        // 3, Seq Search
        while (true)
        {
            offsetIdx++;
            // 找到索引位置
            startIdx = (sliceUptoStart + offsetIdx) & InvertedIndexer.INT_BLOCK_SIZE;
            freqProx = buffer[startIdx];
            docID = freqProx >>> 8;
            if (target < docID || freqProx == 0)
            {
                offsetIdx--;
                break;
            }
        }

        // 找到了最后一个位置 将该位置做为查找的起点
        freqProxPointer = (new Pointer(lp.poolIdx, lp.sliceIdx, offsetIdx)).pointer;
        if (target >= docID)
        {
            return docID;
        }
        return docID = NO_MORE_DOCS;
    }

    /**
     * TODO 采用SkipSlice和二分查找结合的方式 移动到指定位置处 由于位置信息和docId一起存储的
     * 一个docId可以能会有多份proxFreq数据 这里一定要考虑边界问题 要让当前指针跳到的位置是该docId的第一个位置<br/>
     * step 1: 通过跳表的方式 查找到第一个首位freqProx数据中的docId小于等于预期target docId的Slice
     * 来确定target docId 存在的Slice<br/>
     * step 2:
     * 在第一步确定的Slice采用二分查找lowPointer<->nextFreqProxPointer中间第一个文档大于等于target docId
     * 作为当前nextFreqProxPointer<br/>
     * step 3: 顺序往回查找第一个docId小于等于target docId位置作为正在的nextFreqProxPointer<br/>
     * 
     * @param target
     *            预期跳到的文档Id
     * @return docId 第一个小于等于预期Id的文档Id
     */
    public int backward(int target)
    {
        // 获得了pointer 检查该位置处的数据是否已经安全发布 如果没有安全发布
        IntBlockPool intBlockPool;
        // 用于存储当前Slice中第一个freqProx数据的指针
        int lowPointer;
        // 当前数据在当前blcok中的索引位置
        int intUptoStart;
        // 当前buffer在block中的索引
        int bufferIdx;
        // 当前buffer
        int[] buffer;
        // 当前数据在buffer中的索引
        int startIdx;
        // 数据存储信息
        int freqProx;

        boolean isSkiped = false; // 是否有skip slice 操作
        // 1,Skip Search
        while (true)
        {
            Pointer p = new Pointer(freqProxPointer);
            // 获得当前slice所在的block pool
            intBlockPool = plStore.intBlockPools[p.poolIdx];
            // 当前slice在该intBlockPool中intUptoStart第一个存储freqProx的位置
            intUptoStart = p.sliceIdx * PostingListStore.INT_SLICE_SIZE[p.poolIdx] + 1;

            bufferIdx = intUptoStart >>> InvertedIndexer.INT_BLOCK_SHIFT;
            buffer = intBlockPool.buffers[bufferIdx];
            startIdx = intUptoStart & InvertedIndexer.INT_BLOCK_MASK;
            // 当前的数据
            freqProx = buffer[startIdx];
            // 当前的文档Id
            docID = freqProx >>> 8;

            if (docID <= target)
            {// 如果该当前文档Id小于或者等于目标文档Id
             // 说明第一个小于target的文档IdfreqProx数据正在该Slice中
                lowPointer = (new Pointer(p.poolIdx, p.sliceIdx, 1)).pointer;
                break;
            } else
            { // 如果当前的slice中第一个docId还比target文档Id大 就跳到上一个slice
                freqProxPointer = buffer[startIdx - 1];
                Pointer sp = new Pointer(p.poolIdx, p.sliceIdx, 0);
                isSkiped = true;
                if (sp.pointer == startFreqProxPointer)
                {// NO MORE DATA
                    return NO_MORE_DOCS;
                }

            }
        }

        // 找到了 最后一个slice 再看看该slice数据已经写到哪儿了
        Pointer lp = new Pointer(lowPointer);
        Pointer hp = new Pointer(freqProxPointer);
        // 当前slice在当前buffer的开始位置
        int sliceUptoStart = lp.sliceIdx * PostingListStore.INT_SLICE_SIZE[lp.poolIdx];
        int offsetIdx;

        // 2.01,smart skip list search
        if (!isSkiped)
        {
            for (int i = 0; i < skipListIdx; i++)
            {
                if (target <= skipListDocIds[i])
                {// 找到了最大段
                    hp = new Pointer(skipListPointers[i]);
                    break;
                } else
                {// 该处可能是下限
                    lp = new Pointer(skipListPointers[i]);
                }
            }
        }

        // 2,Binary Search
        // 增加一个额外的skipList 每次二分的时候将中间节点的docId,pointer 存入 下次再查找的时候 利用上
        skipListDocIds = new int[4];
        skipListPointers = new int[4];
        skipListIdx = 0;
        while (true)
        {
            // 分到中间的位置
            offsetIdx = (lp.offsetIdx + hp.offsetIdx) / 2;
            if (lp.offsetIdx == offsetIdx || hp.offsetIdx == offsetIdx)
            {// 没有找到和target相同的docID 但是已经搜索完成了 就找最接近的了
                offsetIdx = lp.offsetIdx;
                break;
            }

            // 找到索引位置
            startIdx = (sliceUptoStart + offsetIdx) & InvertedIndexer.INT_BLOCK_MASK;
            freqProx = buffer[startIdx];
            // 解码当前的docId信息 如果为0 或者大于maxDocId 都要再查找
            docID = freqProx >>> 8;
            if (freqProx == 0 || docID > target)
            {// 如果数据为空 或者当前docID大于目标值 就二分到下面查找 说明目标值在低半段
                hp = new Pointer(lp.poolIdx, lp.sliceIdx, offsetIdx);
                // 还在往前找 说明该出数据有可能被下一次查询使用到 放入到skipList当中
                if (skipListIdx < 4)
                {
                    skipListDocIds[skipListIdx] = docID;
                    skipListPointers[skipListIdx] = hp.pointer;
                    skipListIdx++;
                }
            } else if (docID < target)
            {// 如果当前数据比目标数据还小 说明目标值在高半段
                lp = new Pointer(lp.poolIdx, lp.sliceIdx, offsetIdx);
            } else
            {// 找到数据了 就是这里啦 不要跑了
                break;
            }
        }

        // 3,Seq Search
        while (true)
        {// 可能会有多个位置信息 我们要找到第一个为该docId的
            offsetIdx++;
            // TODO offsetIdx考虑在slice内越界
            // 找到索引位置
            startIdx = (sliceUptoStart + offsetIdx) & InvertedIndexer.INT_BLOCK_MASK;
            freqProx = buffer[startIdx];
            docID = freqProx >>> 8;
            if (target < docID || freqProx == 0)
            {// 已经越界了 不要再跑
                offsetIdx--;
                break;
            }
        }

        startIdx = (sliceUptoStart + offsetIdx) & InvertedIndexer.INT_BLOCK_MASK;
        freqProx = buffer[startIdx];
        docID = freqProx >>> 8;

        // 找到了最后一个位置 将该位置做为查找的起点
        freqProxPointer = (new Pointer(lp.poolIdx, lp.sliceIdx, offsetIdx)).pointer;

        if (target >= docID)
        {
            return docID;
        }
        return docID = NO_MORE_DOCS;
    }

    public int nextPosition()
    {
        // 下一个位置信息
        if (positionIndex >= docFreq)
        {
            System.out.println("some error! no more position info!");
            return -1;
        }
        return position[positionIndex++];
    }

    public int freq()
    {// 该term在该文档下面出现的频率
        return docFreq;
    }

    public int docID()
    {
        return docID;
    }

    /**
     * 该方法和backward的不同是 backward只找到最后一次存储该term的位置<br/>
     * 而advance要获得位置信息并把指针指到下一个docId处<br/>
     * 采用SkipSlice和二分查找结合的方式 移动到指定位置处 由于位置信息和docId一起存储的<br/>
     * 一个docId可以能会有多份proxFreq数据 这里一定要考虑边界问题 要让当前指针跳到的位置是该docId的第一个位置<br/>
     * step 1: 通过跳表的方式 查找到第一个首位freqProx数据中的docId小于等于预期target docId的Slice
     * 来确定target docId 存在的Slice<br/>
     * step 2:
     * 在第一步确定的Slice采用二分查找lowPointer<->nextFreqProxPointer中间第一个文档大于等于target docId
     * 作为当前nextFreqProxPointer<br/>
     * step 3: 顺序往回查找第一个docId小于等于target docId位置作为正在的nextFreqProxPointer<br/>
     * 
     * @param target
     *            预期跳到的文档Id
     * @return docId 第一个小于等于预期Id的文档Id
     */
    public int advance(int target)
    {
        Pointer startPointer = new Pointer(startFreqProxPointer);
        Pointer currentPointer = new Pointer(freqProxPointer);
        boolean isEnd = true;
        if (startPointer.poolIdx < currentPointer.poolIdx)
        {// pool 不一致
            isEnd = false;
        } else if (startPointer.sliceIdx < currentPointer.sliceIdx)
        {// 不在同一slice
            isEnd = false;
        } else if (startPointer.offsetIdx + 1 < currentPointer.offsetIdx)
        {// 同一slice
            isEnd = false;
        }

        if (isEnd)
        {
            return NO_MORE_DOCS;
        } else
        {
            int docId = backward(target);
            if (docId == NO_MORE_DOCS)
            {
                return NO_MORE_DOCS;
            }

            while (bits.get(docId) == false)// 该文档已经被删除了 再找下一个
            {
                docId--;
                docId = backward(target);
            }

            return docId;
        }

    }
}
