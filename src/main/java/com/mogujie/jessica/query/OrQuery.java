package com.mogujie.jessica.query;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

public class OrQuery extends ListQuery
{

    private static final long serialVersionUID = 1L;

    public OrQuery(final QueryNode... queryNodes)
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
        Joiner joiner = Joiner.on(" OR ");
        return joiner.join(foo);
    }

    public QueryNode duplicate()
    {
        QueryNode qn = new OrQuery(this.queryNodes.toArray(new QueryNode[] {}));
        return qn;
    }

}
