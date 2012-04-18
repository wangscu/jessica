package com.mogujie.jessica.util;

public interface Bits
{
    public boolean get(int index);

    public int length();

    public static final Bits[] EMPTY_ARRAY = new Bits[0];

    public static class MatchAllBits implements Bits
    {
        final int len;

        public MatchAllBits(int len)
        {
            this.len = len;
        }

        public boolean get(int index)
        {
            return true;
        }

        public int length()
        {
            return len;
        }
    }

    public static class MatchNoBits implements Bits
    {
        final int len;

        public MatchNoBits(int len)
        {
            this.len = len;
        }

        public boolean get(int index)
        {
            return false;
        }

        public int length()
        {
            return len;
        }
    }
}
