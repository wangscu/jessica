package com.mogujie.jessica.query;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * 一个包含多个叶节点的查询节点 具体没有查询 而是组合子查询结果<br/>
 * 以知的查询有或查询 与查询<br/>
 * NOT查询 也是通过全部集合减去正向的term的集合获得<br/>
 * 
 * @author xuanxi
 * 
 */
public abstract class ListQuery extends QueryNode
{

    private static final long serialVersionUID = 1L;
    protected final List<QueryNode> queryNodes;

    public ListQuery(final QueryNode... queryNodes)
    {
        if (null == queryNodes)
            throw new IllegalArgumentException("constructor: left term must not be null");
        this.queryNodes = Arrays.asList(queryNodes);
    }

    @Override
    public Set<TermQuery> getPositiveTerms()
    {
        Set<TermQuery> l = null;
        for (QueryNode query : queryNodes)
        {
            Set<TermQuery> r = query.getPositiveTerms();
            if (l == null)
            {
                l = r;
            } else
            {
                l.addAll(r);
            }
        }
        return l;
    }

    public List<QueryNode> getQueries()
    {
        return this.queryNodes;
    }

    @Override
    public Iterable<QueryNode> getChildren()
    {
        return Sets.newHashSet(queryNodes);
    }

    @Override
    public int hashCode()
    {
        int result = 0;
        for (QueryNode query : queryNodes)
        {
            result += (query == null) ? 0 : query.hashCode();
        }
        result = result ^ super.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!super.equals(obj))
            return false;
        ListQuery other = (ListQuery) obj;
        if (other.getQueries().size() != this.queryNodes.size())
        {
            return false;
        }

        for (int i = 0; i < this.queryNodes.size(); i++)
        {
            if (!this.queryNodes.get(i).equals(other.getQueries().get(i)))
            {
                return false;
            }
        }
        return true;
    }

}
