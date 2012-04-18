package com.mogujie.jessica.util;

/**
 * Returns primitive memory sizes for estimating RAM usage.
 * 
 */
public abstract class MemoryModel
{

    /**
     * @return size of array beyond contents
     */
    public abstract int getArraySize();

    /**
     * @return Class size overhead
     */
    public abstract int getClassSize();

    /**
     * @param clazz
     *            a primitive Class - bool, byte, char, short, long, float,
     *            short, double, int
     * @return the size in bytes of given primitive Class
     */
    public abstract int getPrimitiveSize(Class<?> clazz);

    /**
     * @return size of reference
     */
    public abstract int getReferenceSize();

}
