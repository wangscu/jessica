package com.mogujie.jessica.query;

public final class RawMatch implements Comparable<RawMatch>
{
    private int rawId;
    private double score;

    public RawMatch(int rawId, double score)
    {
        this.rawId = rawId;
        this.score = score;
    }

    public int compareTo(RawMatch o)
    {
        return rawId - o.rawId;
    }

    public int getRawId()
    {
        return rawId;
    }

    public double getScore()
    {
        return score;
    }

    public void setRawId(int rawId)
    {
        this.rawId = rawId;
    }

    public void setScore(double score)
    {
        this.score = score;
    }

    public String toString()
    {
        return "rawId: " + rawId + " score: " + score;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + rawId;
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
        RawMatch other = (RawMatch) obj;
        if (rawId != other.rawId)
            return false;
        return true;
    }

}
