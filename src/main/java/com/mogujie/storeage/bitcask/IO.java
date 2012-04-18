package com.mogujie.storeage.bitcask;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** Utility class for I/O operations */
public class IO
{

    public static long read(FileChannel ch, ByteBuffer[] bb, final long start_position) throws IOException
    {

        long position = start_position;
        for (int i = 0; i < bb.length; i++)
        {

            ByteBuffer buf = bb[i];
            while (buf.hasRemaining())
            {

                int read = ch.read(buf, position);
                if (read >= 0)
                {
                    position += read;

                    if (read == 0)
                    {
                        Thread.yield();
                    }
                } else
                {
                    return 0;
                }
            }
        }

        return position - start_position;
    }

    public static long read(FileChannel ch, ByteBuffer bb, final long start_position) throws IOException
    {

        long position = start_position;
        while (bb.hasRemaining())
        {

            int read = ch.read(bb, position);
            if (read >= 0)
            {
                position += read;

                if (read == 0)
                {
                    Thread.yield();
                }
            } else
            {
                return 0;
            }
        }

        return position - start_position;
    }

    public static long write_fully(FileChannel ch, ByteBuffer[] vec) throws IOException
    {

        synchronized (ch)
        {

            long len = length(vec);

            long w = 0;

            while (w < len)
            {
                long ww = ch.write(vec);
                if (ww > 0)
                {
                    w += ww;
                } else if (ww == 0)
                {
                    Thread.yield();
                } else
                {
                    return w;
                }
            }

            return w;
        }

    }

    private static long length(ByteBuffer[] vec)
    {
        long length = 0;
        for (int i = 0; i < vec.length; i++)
        {
            length += vec[i].remaining();
        }
        return length;
    }

}
