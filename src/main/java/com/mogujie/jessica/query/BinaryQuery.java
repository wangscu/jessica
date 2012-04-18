package com.mogujie.jessica.query;

import java.util.Set;

import com.google.common.collect.Sets;

/**
 * 一个包含2个叶节点的查询节点 具体没有查询 而是组合子查询结果<br/>
 * 以知的查询有或查询 与查询<br/>
 * NOT查询 也是通过全部集合减去正向的term的集合获得<br/>
 * 
 * @author xuanxi
 * 
 */
public abstract class BinaryQuery extends QueryNode
{

    private static final long serialVersionUID = 1L;

    protected final QueryNode leftQuery;
    protected final QueryNode rightQuery;

    public BinaryQuery(final QueryNode lq, final QueryNode rq)
    {
        if (null == lq)
            throw new IllegalArgumentException("constructor: left term must not be null");
        if (null == rq)
            throw new IllegalArgumentException("constructor: right term must not be null");
        leftQuery = lq;
        rightQuery = rq;
    }

    @Override
    public Set<TermQuery> getPositiveTerms()
    {
        Set<TermQuery> l = leftQuery.getPositiveTerms();
        Set<TermQuery> r = rightQuery.getPositiveTerms();
        l.addAll(r);
        return l;
    }

    public QueryNode getLeftQuery()
    {
        return leftQuery;
    }

    public QueryNode getRightQuery()
    {
        return rightQuery;
    }

    @Override
    public Iterable<QueryNode> getChildren()
    {
        return Sets.newHashSet(leftQuery, rightQuery);
    }

    @Override
    public int hashCode()
    {
        int result = 0;
        result += ((leftQuery == null) ? 0 : leftQuery.hashCode());
        result += ((rightQuery == null) ? 0 : rightQuery.hashCode());
        result = result ^ super.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!super.equals(obj))
            return false;
        BinaryQuery other = (BinaryQuery) obj;

        return ((leftQuery.equals(other.leftQuery) && rightQuery.equals(other.rightQuery)) || (leftQuery.equals(other.rightQuery) && rightQuery.equals(other.leftQuery)));
    }
}
