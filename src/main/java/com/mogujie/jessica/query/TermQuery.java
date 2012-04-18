package com.mogujie.jessica.query;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/**
 * Query that searches for a specific term in a specific field. The term passed
 * to the constructor does not suffer any modifications, no case conversions, no
 * tokenization. So great care has to be taken to be sure the term passed is
 * consistent with the tokenization made at index time.
 * 
 * @author spike
 * 
 */
public class TermQuery extends QueryNode implements Serializable
{

    private static final long serialVersionUID = 1L;
    protected String field;
    protected String term;
    protected float boost;

    /**
     * Basic constructor.
     * 
     * @param field
     *            the field where to look the term in. Must not be null.
     * @param term
     *            the term to search for. Must not be null.
     * @throws IllegalArgumentException
     *             if term or field are null.
     */
    public TermQuery(final String field, final String term)
    {
        if (null == field)
            throw new IllegalArgumentException("constructor: field must not be null.");
        if (null == term)
            throw new IllegalArgumentException("constructor: term must not be null.");
        this.field = field;
        this.term = term;
    }

    public String getField()
    {
        return field;
    }

    public String getTerm()
    {
        return term;
    }

    public void setField(String field)
    {
        this.field = field;
    }

    public void setTerm(String term)
    {
        this.term = term;
    }

    @Override
    public Set<TermQuery> getPositiveTerms()
    {
        Set<TermQuery> retval = new HashSet<TermQuery>();
        retval.add(this);
        return retval;
    }

    @Override
    public String toString()
    {
        return "field: " + field + "; term: " + term;
    }

    @Override
    public boolean equals(final Object obj)
    {
        if (!super.equals(obj))
            return false;
        TermQuery tq = (TermQuery) obj;
        return field.equals(tq.field) && term.equals(tq.term);
    }

    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 17 * hash + field.hashCode();
        hash = 17 * hash + term.hashCode();
        hash = hash ^ super.hashCode();
        return hash;
    }

    @Override
    public QueryNode duplicate()
    {
        QueryNode qn = new TermQuery(this.field, this.term);
        return qn;
    }

}
