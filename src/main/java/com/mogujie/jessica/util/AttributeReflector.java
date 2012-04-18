package com.mogujie.jessica.util;

/**
 * This interface is used to reflect contents of {@link AttributeSource} or
 * {@link AttributeImpl}.
 */
public interface AttributeReflector
{

    /**
     * This method gets called for every property in an {@link AttributeImpl}/
     * {@link AttributeSource} passing the class name of the {@link Attribute},
     * a key and the actual value. E.g., an invocation of
     * {@link org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl#reflectWith}
     * would call this method once using
     * {@code org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class}
     * as attribute class, {@code "term"} as key and the actual value as a
     * String.
     */
    public void reflect(Class<? extends Attribute> attClass, String key, Object value);

}
