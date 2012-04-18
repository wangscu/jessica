package com.mogujie.jessica.util;


/**
 * This attribute is requested by TermsHashPerField to index the contents. This
 * attribute can be used to customize the final byte[] encoding of terms.
 * <p>
 * Consumers of this attribute call {@link #getBytesRef()} up-front, and then
 * invoke {@link #fillBytesRef()} for each term. Example:
 * 
 * <pre class="prettyprint">
 *   final TermToBytesRefAttribute termAtt = tokenStream.getAttribute(TermToBytesRefAttribute.class);
 *   final BytesRef bytes = termAtt.getBytesRef();
 * 
 *   while (termAtt.incrementToken() {
 * 
 *     // you must call termAtt.fillBytesRef() before doing something with the bytes.
 *     // this encodes the term value (internally it might be a char[], etc) into the bytes.
 *     int hashCode = termAtt.fillBytesRef();
 * 
 *     if (isInteresting(bytes)) {
 *     
 *       // because the bytes are reused by the attribute (like CharTermAttribute's char[] buffer),
 *       // you should make a copy if you need persistent access to the bytes, otherwise they will
 *       // be rewritten across calls to incrementToken()
 * 
 *       doSomethingWith(new BytesRef(bytes));
 *     }
 *   }
 *   ...
 * </pre>
 * 
 * @lucene.experimental This is a very expert API, please use
 *                      {@link CharTermAttributeImpl} and its implementation of
 *                      this method for UTF-8 terms.
 */
public interface TermToBytesRefAttribute extends Attribute
{
    /**
     * Updates the bytes {@link #getBytesRef()} to contain this term's final
     * encoding, and returns its hashcode.
     * 
     * @return the hashcode as defined by {@link BytesRef#hashCode}:
     * 
     *         <pre>
     * int hash = 0;
     * for (int i = termBytes.offset; i &lt; termBytes.offset + termBytes.length; i++)
     * {
     *     hash = 31 * hash + termBytes.bytes[i];
     * }
     * </pre>
     * 
     *         Implement this for performance reasons, if your code can
     *         calculate the hash on-the-fly. If this is not the case, just
     *         return {@code termBytes.hashCode()}.
     */
    public int fillBytesRef();

    /**
     * Retrieve this attribute's BytesRef. The bytes are updated from the
     * current term when the consumer calls {@link #fillBytesRef()}.
     * 
     * @return this Attributes internal BytesRef.
     */
    public BytesRef getBytesRef();
}
