package com.mogujie.storeage.bitcask;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;

/**
 * @project:杭州卷瓜网络有限公司搜索引擎
 * @date:2011-9-19
 * @edit:2011-9-22
 * @author:xuanxi
 */
public class BitCaskKeyDir
{

    ConcurrentHashMap<ByteString, BitCaskEntry> map = new ConcurrentHashMap<ByteString, BitCaskEntry>();
    private boolean is_ready;

    public boolean put(ByteString key, BitCaskEntry ent)
    {

        BitCaskEntry old = map.get(key);
        if (old == null)
        {
            map.put(key, ent);
            return true;
        } else if (ent.is_newer_than(old))
        {
            map.put(key, ent);
            return true;
        } else
        {
            return false;
        }
    }

    public boolean replace(ByteString key, BitCaskEntry ent)
    {
        map.put(key, ent);
        return true;
    }

    public BitCaskEntry get(ByteString key)
    {
        return map.get(key);
    }

    public synchronized int size()
    {
        return map.size();
    }

    public static ConcurrentHashMap<File, BitCaskKeyDir> key_dirs = new ConcurrentHashMap<File, BitCaskKeyDir>();

    public static BitCaskKeyDir keydir_new(File dirname, int openTimeoutSecs) throws IOException
    {

        File abs_name = dirname.getAbsoluteFile();
        BitCaskKeyDir dir;
        dir = key_dirs.get(abs_name);
        if (dir == null)
        {
            dir = new BitCaskKeyDir();
            key_dirs.put(abs_name, dir);
            return dir;
        }

        if (dir.wait_for_ready(openTimeoutSecs))
        {
            return dir;
        } else
        {
            throw new IOException("timeout while waiting for keydir");
        }
    }

    public static boolean keydir_close(File dirname)
    {
        File abs_name = dirname.getAbsoluteFile();
        if (key_dirs.containsKey(abs_name))
        {
            key_dirs.remove(abs_name);
            return true;
        }
        return false;
    }

    public synchronized boolean is_ready()
    {
        return is_ready;
    }

    public synchronized void mark_ready()
    {
        is_ready = true;
        this.notifyAll();
    }

    public synchronized boolean wait_for_ready(int timeout_secs)
    {
        long now = System.currentTimeMillis();
        long abs_timeout = now + (timeout_secs * 1000);

        while (!is_ready && now < abs_timeout)
        {
            try
            {
                wait();
            } catch (InterruptedException e)
            {
                // ignore
            }

            now = System.currentTimeMillis();
        }

        return is_ready;
    }

}
