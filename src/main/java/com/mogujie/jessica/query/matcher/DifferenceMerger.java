package com.mogujie.jessica.query.matcher;

import com.mogujie.jessica.query.RawMatch;
import com.mogujie.jessica.util.AbstractSkippableIterable;
import com.mogujie.jessica.util.AbstractSkippableIterator;
import com.mogujie.jessica.util.SkippableIterable;
import com.mogujie.jessica.util.SkippableIterator;

class DifferenceMerger extends AbstractSkippableIterable<RawMatch>
{
    private final SkippableIterable<RawMatch> included;
    private final SkippableIterable<RawMatch> excluded;

    DifferenceMerger(SkippableIterable<RawMatch> left, SkippableIterable<RawMatch> right)
    {
        this.included = left;
        this.excluded = right;
    }

    @Override
    public SkippableIterator<RawMatch> iterator()
    {
        return new DifferenceIterator(included.iterator(), excluded.iterator());
    }

    private static class DifferenceIterator extends AbstractSkippableIterator<RawMatch>
    {
        private final SkippableIterator<RawMatch> included;
        private final SkippableIterator<RawMatch> excluded;
        private RawMatch excluedRawMatch = null;

        public DifferenceIterator(SkippableIterator<RawMatch> included, SkippableIterator<RawMatch> excluded)
        {
            this.included = included;
            this.excluded = excluded;
        }

        private static int id(RawMatch r)
        {
            return r.getRawId();
        }

        @Override
        protected RawMatch computeNext()
        {
            if (excluedRawMatch == null && excluded.hasNext())
            {
                excluedRawMatch = excluded.next();
            }

            while (included.hasNext())
            {
                RawMatch candidate = included.next();
                // check if the candidate is excluded
                if (excluedRawMatch != null && id(excluedRawMatch) > id(candidate))
                {// skip exclusions lower than candidate
                    excluded.skipTo(id(candidate));
                    if (excluded.hasNext())
                    {
                        excluedRawMatch = excluded.next();
                    }
                }

                // if candidate was excluded search for another candidate
                if (excluedRawMatch != null && id(candidate) == id(excluedRawMatch))
                {
                    continue;
                }
                return candidate;
            }

            return endOfData();
        }

        @Override
        public void skipTo(int i)
        {
            this.included.skipTo(i);
            this.excluded.skipTo(i - 1);
        }
    }
}
