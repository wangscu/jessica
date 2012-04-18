package com.mogujie.jessica.util;


/**
 * The start and end character offset of a Token.
 */
public interface OffsetAttribute extends Attribute
{
    /**
     * Returns this Token's starting offset, the position of the first character
     * corresponding to this token in the source text.
     * 
     * Note that the difference between endOffset() and startOffset() may not be
     * equal to termText.length(), as the term text may have been altered by a
     * stemmer or some other filter.
     */
    public int startOffset();

    /**
     * Set the starting and ending offset.
     * 
     * @see #startOffset() and #endOffset()
     */
    public void setOffset(int startOffset, int endOffset);

    /**
     * Returns this Token's ending offset, one greater than the position of the
     * last character corresponding to this token in the source text. The length
     * of the token in the source text is (endOffset - startOffset).
     */
    public int endOffset();
}
