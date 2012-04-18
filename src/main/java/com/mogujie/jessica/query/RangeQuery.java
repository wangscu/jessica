package com.mogujie.jessica.query;

/**
 * @author xuanxi
 * 
 */
public class RangeQuery extends QueryNode
{
    private static final long serialVersionUID = 1L;
    private final String field;
    private final int start;
    private final int end;

    public RangeQuery(String field, int start, int end)
    {
        this.field = field;
        this.start = start;
        this.end = end;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!super.equals(obj))
            return false;
        RangeQuery q = (RangeQuery) obj;
        return field.equals(q.field) && start == q.start && end == q.end;
    }

    @Override
    public int hashCode()
    {
        return field.hashCode() ^ start ^ end ^ super.hashCode();
    }

    @Override
    public QueryNode duplicate()
    {
        QueryNode qn = new RangeQuery(this.field, this.start, this.end);
        return qn;
    }

    public String getField()
    {
        return field;
    }

    public int getStart()
    {
        return start;
    }

    public int getEnd()
    {
        return end;
    }

}
