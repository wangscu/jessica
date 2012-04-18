package com.mogujie.jessica.query.matcher;

import com.mogujie.jessica.index.InvertedIndexPerField;
import com.mogujie.jessica.index.InvertedIndexer;
import com.mogujie.jessica.query.DocTermMatch;
import com.mogujie.jessica.query.TermSkippableIterator;
import com.mogujie.jessica.store.PostingList;
import com.mogujie.jessica.util.AbstractSkippableIterable;
import com.mogujie.jessica.util.AbstractSkippableIterator;
import com.mogujie.jessica.util.Bits;
import com.mogujie.jessica.util.BytesRefHash;
import com.mogujie.jessica.util.DocIdSetIterator;
import com.mogujie.jessica.util.SkippableIterable;
import com.mogujie.jessica.util.SkippableIterator;

public class TermMatcher
{
    private final InvertedIndexer indexer;

    public TermMatcher(InvertedIndexer indexer)
    {
        this.indexer = indexer;
    }

    public SkippableIterable<DocTermMatch> getMatches(String field, String term)
    {

        final int maxDocId = indexer.maxDoc();
        final Bits liveDocs = indexer.liveDocs(maxDocId);
        final InvertedIndexPerField inverter = indexer.getIndexPerField(field);
        if (inverter == null)
        {
            return EMPTY_DOC_MATCH;
        }
        final int termId = inverter.getTermId(term);
        if (termId == BytesRefHash.NO_SUCH_TERM)
        {
            return EMPTY_DOC_MATCH;
        }
        final PostingList postingList = new PostingList(termId, maxDocId, inverter.parallelArray, indexer.plStore, liveDocs);
        return new AbstractSkippableIterable<DocTermMatch>()
        {
            @Override
            public SkippableIterator<DocTermMatch> iterator()
            {
                return new TermSkippableIterator(postingList);
            }
        };
    }

    public SkippableIterable<DocTermMatch> getMatches(final String field, final String termFrom, final String termTo)
    {
        return new AbstractSkippableIterable<DocTermMatch>()
        {
            @Override
            public SkippableIterator<DocTermMatch> iterator()
            {

                return new AbstractSkippableIterator<DocTermMatch>()
                {
                    final Bits bits = indexer.getRange(field, termFrom, termTo);
                    int current = indexer.maxDoc();

                    @Override
                    public void skipTo(int i)
                    {
                        current = i;
                    }

                    @Override
                    protected DocTermMatch computeNext()
                    {
                        while (bits.get(current) == false && current != 0)
                        {
                            current--;
                        }
                        if (current == 0)
                        {
                            return endOfData();
                        }

                        int docId = current;
                        if (docId != DocIdSetIterator.NO_MORE_DOCS)
                        {
                            current--;
                            return new DocTermMatch(docId, new int[1], 0);
                        } else
                        {
                            return endOfData();
                        }
                    }
                };
            }
        };
    }

    public SkippableIterable<Integer> getAllDocs()
    {

        return new AbstractSkippableIterable<Integer>()
        {
            @Override
            public SkippableIterator<Integer> iterator()
            {
                return new AbstractSkippableIterator<Integer>()
                {
                    int current;
                    final int maxDoc;
                    final Bits liveDocs;
                    {
                        maxDoc = indexer.maxDoc();
                        liveDocs = indexer.liveDocs(maxDoc);
                        current = maxDoc;
                    }

                    @Override
                    public void skipTo(int i)
                    {
                        current = i;
                    }

                    @Override
                    protected Integer computeNext()
                    {
                        while (current-- > 0)
                        {
                            if (liveDocs.get(current) == false)
                            {
                                return current;
                            }
                        }
                        return endOfData();
                    }
                };
            }
        };
    }

    public boolean hasChanges(Integer docid)
    {
        return false;
    }

    private final static AbstractSkippableIterable<DocTermMatch> EMPTY_DOC_MATCH;
    static
    {
        EMPTY_DOC_MATCH = new AbstractSkippableIterable<DocTermMatch>()
        {
            public SkippableIterator<DocTermMatch> iterator()
            {
                return new AbstractSkippableIterator<DocTermMatch>()
                {
                    public void skipTo(int i)
                    {

                    }

                    protected DocTermMatch computeNext()
                    {
                        return endOfData();
                    }

                };
            }
        };

    }
}
