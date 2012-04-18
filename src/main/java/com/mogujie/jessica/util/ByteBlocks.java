package com.mogujie.jessica.util;


public class ByteBlocks
{
    public byte[][] buffers;

    public ByteBlocks(byte[][] buffers)
    {
        this.buffers = buffers;
    }

    public static ByteBlocks copyRef(ByteBlockPool bbp)
    {
        byte[][] buffers = bbp.buffers;
        return new ByteBlocks(buffers);
    }

    public final BytesRef setBytesRef(BytesRef term, int textStart)
    {
        final byte[] bytes = term.bytes = buffers[textStart >> ByteBlockPool.BYTE_BLOCK_SHIFT];
        int pos = textStart & ByteBlockPool.BYTE_BLOCK_MASK;
        if ((bytes[pos] & 0x80) == 0)
        {
            // length is 1 byte
            term.length = bytes[pos];
            term.offset = pos + 1;
        } else
        {
            // length is 2 bytes
            term.length = (bytes[pos] & 0x7f) + ((bytes[pos + 1] & 0xff) << 7);
            term.offset = pos + 2;
        }
        assert term.length >= 0;
        return term;
    }
}
