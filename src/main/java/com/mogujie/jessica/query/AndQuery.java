package com.mogujie.jessica.query;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

public class AndQuery extends ListQuery
{

    private static final long serialVersionUID = 1L;

    public AndQuery(final QueryNode... queryNodes)
    {
        super(queryNodes);
    }

    @Override
    public String toString()
    {
        List<String> foo = new ArrayList<String>();
        for (QueryNode query : queryNodes)
        {
            foo.add("( " + query.toString() + " ) ");
        }
        Joiner joiner = Joiner.on(" AND ");
        return joiner.join(foo);
    }

    public QueryNode duplicate()
    {
        QueryNode qn = new AndQuery(this.queryNodes.toArray(new QueryNode[] {}));
        return qn;
    }

}
