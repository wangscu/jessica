package com.mogujie.jessica.query.matcher;

import java.util.List;

import org.apache.log4j.Logger;

import com.mogujie.jessica.query.RawMatch;
import com.mogujie.jessica.store.PostingList;
import com.mogujie.jessica.util.AbstractSkippableIterable;
import com.mogujie.jessica.util.AbstractSkippableIterator;
import com.mogujie.jessica.util.Constants;
import com.mogujie.jessica.util.SkippableIterator;

/**
 * 当所有的查询存在值的时候才有值
 * 
 * @author xuanxi
 * 
 */
class AndMerger extends AbstractSkippableIterable<RawMatch>
{
    private final static Logger logger = Logger.getLogger(Constants.LOG_SEARCH);
    private List<SkippableIterator<RawMatch>> list;

    AndMerger(List<SkippableIterator<RawMatch>> list)
    {
        this.list = list;
    }

    @Override
    public SkippableIterator<RawMatch> iterator()
    {
        return new AbstractSkippableIterator<RawMatch>()
        {
            private int nextId = PostingList.NO_MORE_DOCS;

            /**
             * 都跳到指定的位置
             */
            @Override
            public void skipTo(int i)
            {
                nextId = i;
            }

            @Override
            protected RawMatch computeNext()
            {
                while (true)
                {
                    RawMatch rawMatch = null;
                    boolean found = true;
                    for (final SkippableIterator<RawMatch> skip : list)
                    {
                        skip.skipTo(nextId);
                        if (!skip.hasNext())
                        {// one no more data end
                            return endOfData();
                        }

                        rawMatch = skip.next();

                        if (rawMatch.getRawId() == nextId)
                        {// haha got you
                            continue;
                        } else
                        {
                            found = false;
                            nextId = rawMatch.getRawId();// go on
                        }

                    }
                    if (found)
                    {
                        RawMatch r = new RawMatch(nextId, 0);
                        nextId--;// 继续往下找
                        return r;
                    }
                }
            }

        };
    }
}
