package com.mogujie.jessica.query.matcher;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.mogujie.jessica.query.AndQuery;
import com.mogujie.jessica.query.DifferenceQuery;
import com.mogujie.jessica.query.DocTermMatch;
import com.mogujie.jessica.query.MatchAllQuery;
import com.mogujie.jessica.query.OrQuery;
import com.mogujie.jessica.query.Query;
import com.mogujie.jessica.query.QueryMatcher;
import com.mogujie.jessica.query.QueryNode;
import com.mogujie.jessica.query.RangeQuery;
import com.mogujie.jessica.query.RawMatch;
import com.mogujie.jessica.query.SimplePhraseQuery;
import com.mogujie.jessica.query.SimpleUids;
import com.mogujie.jessica.query.TermQuery;
import com.mogujie.jessica.query.TerminableTermQuery;
import com.mogujie.jessica.util.AbstractSkippableIterable;
import com.mogujie.jessica.util.SkippableIterable;
import com.mogujie.jessica.util.SkippableIterator;
import com.mogujie.jessica.util.Skippables;
import com.mogujie.jessica.util.TerminalbleSkippableIterator;

public class TermBasedQueryMatcher implements QueryMatcher
{

    private final TermMatcher matcher;
    private final int doc2uidArray[];

    public TermBasedQueryMatcher(TermMatcher matcher, int doc2uidArray[])
    {
        this.matcher = matcher;
        this.doc2uidArray = doc2uidArray;
    }

    /*
     * 返回与查询相关的所有文档 顺序是从新到旧 并没有进行排序处理
     */
    @Override
    public SimpleUids getAllMatches(Query query, int limit)
    {
        Iterable<RawMatch> rawMatches = match(query.getRoot());
        SimpleUids uids = new SimpleUids();
        // 通过文档id获得对应的唯一ids
        for (RawMatch rawMatch : rawMatches)
        {
            int uid = doc2uidArray[rawMatch.getRawId()];
            uids.add(uid);
        }
        return uids;
    }

    public int countMatches(Query query, Predicate<Integer> idFilter)
    {
        return getCount(match(query.getRoot()), idFilter);
    }

    @Override
    public int countMatches(Query query)
    {
        return countMatches(query, Predicates.<Integer> alwaysTrue());
    }

    @Override
    public boolean hasChanges(Integer docid)
    {
        return matcher.hasChanges(docid);
    }

    // TODO 具体实现 计算总数 要走缓存了
    private int getCount(Iterable<RawMatch> rawMatches, Predicate<Integer> docFilter)
    {
        int totalCount = 0;
        return totalCount;
    }

    private SkippableIterable<RawMatch> match(QueryNode query)
    {
        // dispatch to specific methods based on query type
        if (query instanceof TerminableTermQuery)
            return terminableMatchTerm((TerminableTermQuery) query);
        else if (query instanceof TermQuery)
            return matchTerm((TermQuery) query);
        else if (query instanceof AndQuery)
            return matchAnd((AndQuery) query);
        else if (query instanceof OrQuery)
            return matchOr((OrQuery) query);
        else if (query instanceof DifferenceQuery)
            return matchDifference((DifferenceQuery) query);
        else if (query instanceof SimplePhraseQuery)
            return matchPhrase((SimplePhraseQuery) query);
        else if (query instanceof MatchAllQuery)
            return matchAll((MatchAllQuery) query);
        else if (query instanceof RangeQuery)
            return matchRange((RangeQuery) query);
        else
            throw new IllegalArgumentException("Unsupported query type: " + query.getClass());
    }

    // 一个可中断的term查询
    private SkippableIterable<RawMatch> terminableMatchTerm(TerminableTermQuery query)
    {
        final SkippableIterable<DocTermMatch> items = matcher.getMatches(query.getField(), query.getTerm());
        final TerminalbleSkippableIterator<RawMatch> iterator = new TerminalbleSkippableIterator<RawMatch>()
        {
            RawMatch m = new RawMatch(0, 0d);
            SkippableIterator<DocTermMatch> it = items.iterator();

            public void skipTo(int i)
            {
                it.skipTo(i);
            }

            @Override
            public boolean hasNext()
            {
                if (isTerminal())
                {// 如果已经中断 就不容许再向下遍历
                    return false;
                }
                return it.hasNext();
            }

            @Override
            public RawMatch next()
            {
                DocTermMatch dtm = it.next();
                m.setRawId(dtm.getRawId());
                m.setScore(dtm.getTermScore());
                return m;
            }

            @Override
            public void remove()
            {
                it.remove();
            }

        };

        // 开始计算时间
        TerminableTermQuery.ticktock(query, iterator);

        return new AbstractSkippableIterable<RawMatch>()
        {
            public SkippableIterator<RawMatch> iterator()
            {
                return iterator;
            }
        };
    }

    private SkippableIterable<RawMatch> matchTerm(TermQuery query)
    {
        final SkippableIterable<DocTermMatch> items = matcher.getMatches(query.getField(), query.getTerm());
        return new AbstractSkippableIterable<RawMatch>()
        {
            public SkippableIterator<RawMatch> iterator()
            {
                return new SkippableIterator<RawMatch>()
                {
                    RawMatch m = new RawMatch(0, 0d);
                    SkippableIterator<DocTermMatch> it = items.iterator();

                    public void skipTo(int i)
                    {
                        it.skipTo(i);
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return it.hasNext();
                    }

                    @Override
                    public RawMatch next()
                    {
                        DocTermMatch dtm = it.next();
                        m.setRawId(dtm.getRawId());
                        m.setScore(dtm.getTermScore());
                        return m;
                    }

                    @Override
                    public void remove()
                    {
                        it.remove();
                    }
                };
            }
        };
    }

    private SkippableIterable<RawMatch> matchAnd(AndQuery query)
    {
        List<QueryNode> nodes = query.getQueries();
        int size = nodes.size();
        List<SkippableIterator<RawMatch>> list = new ArrayList<SkippableIterator<RawMatch>>();
        for (int i = 0; i < size; i++)
        {
            QueryNode node = nodes.get(i);
            SkippableIterable<RawMatch> skip = match(node);
            list.add(skip.iterator());
        }

        SkippableIterable<RawMatch> am = new AndMerger(list);
        return am;
    }

    private SkippableIterable<RawMatch> matchPhrase(final SimplePhraseQuery query)
    {
        String field = query.getField();
        List<String> terms = query.getTerms();
        int[] termPositions = query.getTermPositions();
        // each term gets converted to its item list by matching it to the given
        // field
        //return new PhraseMerger(Iterables.transform(terms, getFieldMatcher(field)), termPositions);
        return null;
    }

    private SkippableIterable<RawMatch> matchOr(OrQuery query)
    {
        List<QueryNode> nodes = query.getQueries();
        int size = nodes.size();
        List<SkippableIterator<RawMatch>> list = new ArrayList<SkippableIterator<RawMatch>>();
        for (int i = 0; i < size; i++)
        {
            QueryNode node = nodes.get(i);
            SkippableIterable<RawMatch> skip = match(node);
            list.add(skip.iterator());
        }

        SkippableIterable<RawMatch> om = new OrMerger(list);
        return om;
    }

    private SkippableIterable<RawMatch> matchDifference(DifferenceQuery query)
    {
        SkippableIterable<RawMatch> left = match(query.getLeftQuery());
        SkippableIterable<RawMatch> right = match(query.getRightQuery());
        return new DifferenceMerger(left, right);
    }

    private SkippableIterable<RawMatch> matchAll(MatchAllQuery query)
    {
        return Skippables.transform(matcher.getAllDocs(), new Function<Integer, RawMatch>()
        {
            @Override
            public RawMatch apply(Integer i)
            {
                return new RawMatch(i, 1d);
            }
        });
    }

    /**
     * 
     * rang query
     * 
     * @param query
     * @return iterabale
     */
    private SkippableIterable<RawMatch> matchRange(RangeQuery query)
    {
        final SkippableIterable<DocTermMatch> items = matcher.getMatches(query.getField(), query.getStart() + "", query.getEnd() + "");
        return new AbstractSkippableIterable<RawMatch>()
        {
            public SkippableIterator<RawMatch> iterator()
            {
                return new SkippableIterator<RawMatch>()
                {
                    RawMatch m = new RawMatch(0, 0d);
                    SkippableIterator<DocTermMatch> it = items.iterator();

                    public void skipTo(int i)
                    {
                        it.skipTo(i);
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return it.hasNext();
                    }

                    @Override
                    public RawMatch next()
                    {
                        DocTermMatch dtm = it.next();
                        m.setRawId(dtm.getRawId());
                        m.setScore(dtm.getTermScore());
                        return m;
                    }

                    @Override
                    public void remove()
                    {
                        it.remove();
                    }
                };
            }
        };
    }

    private Function<String, SkippableIterable<DocTermMatch>> getFieldMatcher(final String field)
    {
        return new Function<String, SkippableIterable<DocTermMatch>>()
        {
            @Override
            public SkippableIterable<DocTermMatch> apply(String term)
            {
                return matcher.getMatches(field, term);
            }
        };
    }

}
