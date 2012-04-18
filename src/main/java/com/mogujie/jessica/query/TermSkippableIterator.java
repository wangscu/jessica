package com.mogujie.jessica.query;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.mogujie.jessica.store.PostingList;
import com.mogujie.jessica.util.AbstractSkippableIterator;

public class TermSkippableIterator extends AbstractSkippableIterator<DocTermMatch>
{
    private final static Logger logger = Logger.getLogger(TermSkippableIterator.class);
    private final PostingList postingList;
    private int nextId = PostingList.NO_MORE_DOCS;
    private DocTermMatch m = null;

    public TermSkippableIterator(PostingList postingList)
    {
        this.postingList = postingList;
    }

    private DocTermMatch match(int rawId, int freq) throws IOException
    {
        if (m == null)
        {
            m = new DocTermMatch(rawId, new int[freq], freq);
        } else
        {
            m.setRawId(rawId);
            m.setPositionsLength(freq);
        }
        int[] positions = new int[freq];//
        if (freq > positions.length)
        {
            positions = new int[freq];
            m.setPositions(positions);
        }

        for (int i = 0; i < freq; i++)
        {
            m.getPositions()[i] = postingList.nextPosition();
        }
        return m;
    }

    @Override
    public void skipTo(int i)
    {
        nextId = i;
    }

    @Override
    protected DocTermMatch computeNext()
    {
        try
        {
            int docId = postingList.advance(nextId);
            if (logger.isDebugEnabled())
            {
                logger.debug("pre doc:" + docId);
            }

            if (docId != PostingList.NO_MORE_DOCS)
            {
                int rawId = postingList.docID();
                nextId = rawId - 1;
                int freq = postingList.freq();
                return match(rawId, freq);
            } else
            {
                return endOfData();
            }
        } catch (IOException e)
        {
            logger.error(e.getMessage(), e);
            return endOfData();
        }
    }

}
