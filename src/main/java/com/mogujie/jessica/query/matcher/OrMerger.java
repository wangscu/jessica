package com.mogujie.jessica.query.matcher;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.mogujie.jessica.query.RawMatch;
import com.mogujie.jessica.util.AbstractSkippableIterable;
import com.mogujie.jessica.util.AbstractSkippableIterator;
import com.mogujie.jessica.util.Constants;
import com.mogujie.jessica.util.SkippableIterator;

/**
 * 只要有一个文档有值 就返回
 * 
 * @author xuanxi
 * 
 */
class OrMerger extends AbstractSkippableIterable<RawMatch>
{
    private final static Logger logger = Logger.getLogger(Constants.LOG_SEARCH);
    private List<SkippableIterator<RawMatch>> list;

    OrMerger(List<SkippableIterator<RawMatch>> list)
    {
        this.list = list;
    }

    @Override
    public SkippableIterator<RawMatch> iterator()
    {
        return new AbstractSkippableIterator<RawMatch>()
        {
            private TreeSet<Head> heads = new TreeSet<Head>(new Comparator<Head>()
            {
                @Override
                public int compare(Head h1, Head h2)
                {
                    int compare = h2.r.getRawId() - h1.r.getRawId();
                    if (compare != 0)
                    {
                        return compare;
                    }
                    return h1.position - h2.position;
                }
            });

            private BitSet nextsToDo = new BitSet();

            private List<Head> prebuiltHeads = Lists.newArrayListWithExpectedSize(list.size());
            {
                for (int i = 0; i < list.size(); i++)
                {
                    prebuiltHeads.add(new Head(i, null));
                }
            }

            private Head getHead(int position, RawMatch r)
            {
                Head head = prebuiltHeads.get(position);
                head.r = r;
                return head;
            }

            {
                /*
                 * constructor<br/> Keep all the heads of all the iterators
                 * sorted.
                 */
                for (int i = 0; i < list.size(); i++)
                {
                    SkippableIterator<RawMatch> skip = list.get(i);
                    if (skip.hasNext())
                    {
                        heads.add(getHead(i, skip.next()));
                    }
                }

            }

            @Override
            public void skipTo(int i)
            {
                for (SkippableIterator<RawMatch> skip : list)
                {
                    skip.skipTo(i);
                }
            }

            /**
             * or 查询 有可能返回了一个docId 但是 其他的docId 比他大
             */
            @Override
            protected RawMatch computeNext()
            {
                while (true)
                {
                    int pos = 0;
                    while (true)
                    {
                        pos = nextsToDo.nextSetBit(pos);
                        // 找出treeSet 最前面的
                        if (pos < 0)
                        {
                            // ok no more data
                            break;
                        }
                        if (list.get(pos).hasNext())
                        {
                            heads.add(getHead(pos, list.get(pos).next()));
                        }
                        nextsToDo.clear(pos);
                        pos++;
                    }
                    Head current = heads.pollFirst();
                    if (current == null)
                    {
                        return endOfData();
                    }

                    int position = current.position;
                    nextsToDo.set(position);

                    RawMatch rawMatch = current.r;

                    // 如果其他的iteator 有相同的docId 直接忽视
                    while (heads.size() > 0 && heads.first().r.getRawId() == rawMatch.getRawId())
                    {
                        Head h = heads.pollFirst();
                        nextsToDo.set(h.position);
                    }
                    return rawMatch;
                }
            }
        };
    }

    private class Head
    {
        int position;
        RawMatch r;

        public Head(int position, RawMatch r)
        {
            this.position = position;
            this.r = r;
        }
    }

}
