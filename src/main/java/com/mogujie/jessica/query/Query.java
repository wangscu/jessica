package com.mogujie.jessica.query;

import java.io.Serializable;

/**
 * Wrapper for IndexTank queries, holds query metadata.
 */
public final class Query implements Serializable
{
    private static final long serialVersionUID = 1L;

    private QueryNode root;
    private String originalStr;
    private int now;

    /**
     * Default constructor.
     * 
     * @param originalStr
     *            the original user generated query string, if applicable.
     */
    public Query(QueryNode root, String originalStr)
    {
        this.root = root;
        this.originalStr = originalStr;
        this.now = (int) (System.currentTimeMillis() / 1000);
    }

    /**
     * Abstract method that returns the original user-generated query string.
     * 
     * @return The original user-generated query string, or null.
     */
    public String getOriginalStr()
    {
        return originalStr;
    }

    /**
     * Returns the query root node.
     * 
     * @return the query root node.
     */
    public QueryNode getRoot()
    {
        return root;
    }

    /**
     * Returns the query creation timestamp.
     */
    public int getNow()
    {
        return now;
    }


    public String toString()
    {
        return root.toString();
    }

    public Query duplicate()
    {
        return new Query(this.root.duplicate(), this.originalStr);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + now;
        result = prime * result + ((originalStr == null) ? 0 : originalStr.hashCode());
        result = prime * result + ((root == null) ? 0 : root.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Query other = (Query) obj;
        if (now != other.now)
            return false;
        if (originalStr == null)
        {
            if (other.originalStr != null)
                return false;
        } else if (!originalStr.equals(other.originalStr))
            return false;
        if (root == null)
        {
            if (other.root != null)
                return false;
        } else if (!root.equals(other.root))
            return false;
        return true;
    }

}
