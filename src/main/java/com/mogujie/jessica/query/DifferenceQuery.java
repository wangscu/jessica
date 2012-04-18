package com.mogujie.jessica.query;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Set;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

/**
 * Represents de mathematical difference (lt - rt).
 * 
 * @author Flaptor Development Team
 */
public final class DifferenceQuery extends BinaryQuery implements Serializable
{

    private static final long serialVersionUID = 1L;

    public DifferenceQuery(final QueryNode lt, final QueryNode rt)
    {
        super(lt, rt);
    }

    @Override
    public Set<TermQuery> getPositiveTerms()
    {
        return leftQuery.getPositiveTerms();
    }

    @Override
    public String toString()
    {
        return "( " + leftQuery.toString() + " ) - ( " + rightQuery.toString() + " )";
    }

    /**
     * Implements a very simple equality. a - b != b - a
     */
    @Override
    public boolean equals(final Object obj)
    {
        if (!super.equals(obj))
            return false;
        BinaryQuery bq = (BinaryQuery) obj;
        if (leftQuery.equals(bq.leftQuery) && rightQuery.equals(bq.rightQuery))
        {
            return true;
        } else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        int hash = 17;
        hash = 3 * hash + leftQuery.hashCode();
        hash = 3 * hash + rightQuery.hashCode();
        hash = hash ^ super.hashCode();
        return hash;
    }

    public QueryNode duplicate()
    {
        QueryNode qn = new DifferenceQuery(this.leftQuery.duplicate(), this.rightQuery.duplicate());
        return qn;
    }

}
