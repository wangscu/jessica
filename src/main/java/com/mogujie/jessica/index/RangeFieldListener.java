package com.mogujie.jessica.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.mogujie.jessica.store.Pointer;
import com.mogujie.jessica.store.PostingListStore;
import com.mogujie.jessica.util.Bits;
import com.mogujie.jessica.util.BytesRef;
import com.mogujie.jessica.util.Constants;
import com.mogujie.jessica.util.OpenBitSet;

/**
 * TODO 当invertedIndexer 压缩的时候 range 缓存也要发生变化<br/>
 * 目前只支持int类型的rangQuery
 * 
 * @author xuanxi
 * 
 */
public class RangeFieldListener
{
    private final static Logger logger = Logger.getLogger(Constants.LOG_INDEX);
    final InvertedIndexer indexer;
    final private ConcurrentHashMap<String, OpenBitSet> ranges = new ConcurrentHashMap<String, OpenBitSet>();
    final private ConcurrentHashMap<String, List<RangeDo>> fieldRanges = new ConcurrentHashMap<String, List<RangeDo>>();

    /**
     * 将旧的range数据转移到新的range中
     * 
     * @param indexer
     * @param rangeFieldListener
     */
    public RangeFieldListener(InvertedIndexer indexer, RangeFieldListener rangeFieldListener, int[] old2doc, int maxDoc)
    {
        this.indexer = indexer;
        fieldRanges.putAll(rangeFieldListener.fieldRanges);
        for (Entry<String, OpenBitSet> entry : rangeFieldListener.ranges.entrySet())
        {
            OpenBitSet oldBits = entry.getValue();
            OpenBitSet bits = new OpenBitSet(1 << 24);
            for (int i = 1; i <= maxDoc; i++)
            {
                if (old2doc[i] > 0 && oldBits.fastGet(i))
                {
                    bits.fastSet(old2doc[i]);
                }
            }
            ranges.put(entry.getKey(), bits);
        }

    }

    public RangeFieldListener(InvertedIndexer indexer)
    {
        this.indexer = indexer;
    }

    public synchronized void newRange(RangeDo rangeDo)
    {
        // dobule check
        OpenBitSet bits = ranges.get(rangeDo.toString());
        if (bits != null)
        {
            return;
        }

        bits = new OpenBitSet(1 << 24);// 直接分配一个1<<24大小的数组

        // 放入map中
        ranges.put(rangeDo.toString(), bits);
        List<RangeDo> list = fieldRanges.get(rangeDo.field);
        if (list == null)
        {
            list = new ArrayList<RangeFieldListener.RangeDo>();
            fieldRanges.putIfAbsent(rangeDo.field, list);
            list = fieldRanges.get(rangeDo.field);
        }
        list.add(rangeDo);

        InvertedIndexPerField perField = indexer.getIndexPerField(rangeDo.field);
        if (perField == null)
        {
            logger.error("no such field " + rangeDo.field + " to init range query!");
            return;
        }

        int maxTerm = perField.maxTerm();
        int maxDoc = indexer.maxDoc();
        for (int i = 0; i <= maxTerm; i++)
        {
            int textStart = perField.parallelArray.textStarts[i];
            BytesRef bytesRef = new BytesRef();
            indexer.termPool.setBytesRef(bytesRef, textStart);
            String term = bytesRef.utf8ToString();
            int value = Integer.MAX_VALUE;
            try
            {
                value = Integer.parseInt(term);

            } catch (Exception e)
            {
                // ignore
            }

            if (value >= rangeDo.start && value <= rangeDo.end)
            {
                Pointer p = new Pointer(perField.parallelArray.freqProxStarts[i]);
                int nextFreqProxPointer = new Pointer(p.poolIdx, p.sliceIdx, 1).pointer;
                // 获得了pointer 检查该位置处的数据是否已经安全发布 如果没有安全发布
                IntBlockPool intBlockPool;
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

                while (true)
                {
                    p = new Pointer(nextFreqProxPointer);
                    // 获得当前slice所在的block pool
                    intBlockPool = indexer.plStore.intBlockPools[p.poolIdx];
                    // 当前slice在该intBlockPool中intUptoStart第一个存储freqProx的位置
                    intUptoStart = p.sliceIdx * PostingListStore.INT_SLICE_SIZE[p.poolIdx] + p.offsetIdx;

                    bufferIdx = intUptoStart >>> InvertedIndexer.INT_BLOCK_SHIFT;
                    buffer = intBlockPool.buffers[bufferIdx];
                    startIdx = intUptoStart & InvertedIndexer.INT_BLOCK_MASK;
                    // 如果当前存储的是指针 即当前Slice的最有一个位置 将nextFreqProxPointer指向当前指针
                    if (p.offsetIdx == PostingListStore.INT_SLICE_SIZE[p.poolIdx] - 1)
                    {
                        nextFreqProxPointer = buffer[startIdx];
                        continue;
                    }
                    // 当前的数据
                    freqProx = buffer[startIdx];
                    if (freqProx == 0)
                    {// 已经没有数据了 哪儿出了异常
                        System.out.println("reset, no more data!");
                        break;
                    } else
                    {
                        // 当前的文档Id
                        int docId = freqProx >>> 8;
                        if (docId > maxDoc)
                        {
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("compact more data here!");
                            }
                            break;
                        }
                        bits.fastSet(docId);
                    }
                }
            }

        }

    }

    /**
     * 有新的数据进来 开始放入ranges中
     */
    public void newValue(int docId, String field, String term)
    {
        List<RangeDo> list = fieldRanges.get(field);
        Integer termValue = Integer.parseInt(term);
        if (termValue == null)
        {
            termValue = 0;
        }

        for (RangeDo rangeDo : list)
        {
            if (rangeDo.start <= termValue && rangeDo.end >= termValue)
            {
                OpenBitSet openBitSet = ranges.get(rangeDo.toString());
                openBitSet.fastSet(docId);
            }
        }
    }

    public Bits getRange(String field, int start, int end)
    {

        String key = field + "_" + start + "_" + end;

        Bits bits = ranges.get(key);
        if (bits == null)
        {// 第一次没有数据 好戏来啦
            RangeDo rangeDo = new RangeDo(field, start, end);
            newRange(rangeDo);
            bits = ranges.get(key);
        }
        return bits;
    }

    private class RangeDo
    {
        public String field;
        public int start;
        public int end;

        public RangeDo(String field, int start, int end)
        {
            this.field = field;
            this.start = start;
            this.end = end;
        }

        public String toString()
        {
            return field + "_" + start + "_" + end;
        }
    }

}
