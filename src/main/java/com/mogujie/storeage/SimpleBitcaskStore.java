package com.mogujie.storeage;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.protobuf.ByteString;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.mogujie.jessica.service.thrift.Doc;
import com.mogujie.storeage.bitcask.BitCask;
import com.mogujie.storeage.bitcask.BitCaskFile;
import com.mogujie.storeage.bitcask.BitCaskOptions;

/**
 * @project:杭州卷瓜网络有限公司搜索引擎
 * @date:2011-9-19
 * @edit:2011-9-22
 * @author:xuanxi
 */
public class SimpleBitcaskStore implements Store, Runnable
{
    private static final Logger log = Logger.getLogger(SimpleBitcaskStore.class);
    private static final String SUFFIX = ".bitcaskstore";
    private File _dataDir;
    private BitCask _bitCask;
    // private CompartorCache _compartorCache;
    private BitCaskOptions _opts;
    private ConcurrentLinkedHashMap<Long, Map<String, String>> _cache;
    private static final int DFAULT_CACHE_SIZE = 250000; // 150000的大小
    private static final int DATA_BLOCK_SIZE = 64 * 1024 * 1024;// 64m
    private volatile boolean _stop = false;
    private int _status = 0;
    private long _storeVersion = 0;
    private AtomicLong _eviction = new AtomicLong(0);
    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private int day = 0;

    public SimpleBitcaskStore(File dir)
    {
        _dataDir = dir;
        if (!_dataDir.exists())
        {
            _dataDir.mkdirs();
        }
        _opts = new BitCaskOptions();
        _opts.read_write = true;
        _opts.max_file_size = DATA_BLOCK_SIZE;
        _cache = new ConcurrentLinkedHashMap.Builder().listener(new StoreEvictionListener()).maximumWeightedCapacity(DFAULT_CACHE_SIZE).build();
    }

    static Pattern DATASTORE_DIR = Pattern.compile("[0-9]+" + SUFFIX);

    public void start()
    {
        try
        {

            File[] files = _dataDir.listFiles(new FileFilter()
            {
                public boolean accept(File f)
                {
                    return DATASTORE_DIR.matcher(f.getName()).matches();
                }
            });

            Arrays.sort(files, 0, files.length, REVERSE_DATA_FILE_COMPARATOR);
            File currentDir = null;
            if (files.length > 0)
            {
                currentDir = files[0]; // 取最新的数据目录
            } else
            {
                _storeVersion = tstamp();
                currentDir = new File(_dataDir, _storeVersion + SUFFIX);
                currentDir.mkdirs();
            }

            log.info("启动bitcask存储服务! 当前数据目录为:" + currentDir.getAbsolutePath());

            _bitCask = BitCask.open(currentDir, _opts);
            Thread thread = new Thread(this);
            thread.setDaemon(true);
            thread.start();
        } catch (Exception e)
        {
            log.error(e.getMessage(), e);
        }

    }

    @Override
    public void shutdown()
    {
        try
        {
            _stop = true;
            _bitCask.close();
        } catch (IOException e)
        {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public int getStatus()
    {
        return _status;
    }

    public void setStatus(int status)
    {
        _status = status;
    }

    @Override
    public int getDocSize()
    {
        return _bitCask.size();
    }

    @Override
    public String getStorePath()
    {
        return _dataDir.getAbsolutePath();
    }

    @Override
    public Map<String, String> get(Long key)
    {

        if (_cache.containsKey(key))
        {
            return _cache.get(key);
        }

        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bos);
            os.writeLong(key);
            os.flush();
            bos.flush();
            byte[] keyByte = bos.toByteArray();
            os.close();
            bos.close();
            ByteString keyString = ByteString.copyFrom(keyByte);
            ByteString dataString = null;
            ReadLock rLock = rwLock.readLock();
            rLock.lock();
            try
            {
                dataString = _bitCask.get(keyString);
            } finally
            {
                rLock.unlock();
            }
            TDeserializer dserializer = new TDeserializer(new TBinaryProtocol.Factory());
            if (dataString != null)
            {
                Doc doc = new Doc();
                dserializer.deserialize(doc, dataString.toByteArray());
                Map<String, String> data = doc.getFields();
                _cache.put(key, data);
                return data;
            }
        } catch (Exception e)
        {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public void put(Long key, Map<String, String> data)
    {
        Doc doc = new Doc();
        doc.setUid(key);
        doc.setFields(data);
        try
        {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            byte[] bytes = serializer.serialize(doc);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bos);
            os.writeLong(doc.getUid());
            os.flush();
            bos.flush();
            byte[] keyByte = bos.toByteArray();
            os.close();
            bos.close();
            ByteString keyString = ByteString.copyFrom(keyByte);
            ByteString dataString = ByteString.copyFrom(bytes);
            _bitCask.put(keyString, dataString);

            _cache.replace(doc.getUid(), doc.getFields());
        } catch (Exception e)
        {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void putAll(List<Doc> docs)
    {
        for (Doc doc : docs)
        {
            try
            {
                TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
                byte[] bytes = serializer.serialize(doc);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream os = new ObjectOutputStream(bos);
                os.writeLong(doc.getUid());
                os.flush();
                bos.flush();
                byte[] keyByte = bos.toByteArray();
                os.close();
                bos.close();
                ByteString keyString = ByteString.copyFrom(keyByte);
                ByteString dataString = ByteString.copyFrom(bytes);
                _bitCask.put(keyString, dataString);

                _cache.replace(doc.getUid(), doc.getFields());
            } catch (Exception e)
            {
                log.error(e.getMessage(), e);
            }

        }
    }

    @Override
    public void setCacheSize(int size)
    {
        _cache.setCapacity(size);
    }

    @Override
    public int getCacheSize()
    {
        return _cache.size();
    }

    public int capacity()
    {
        return _cache.capacity();
    }

    public long diskSize()
    {
        return _bitCask.diskSize();
    }

    /**
     * 异步更新接口 保证只有一个线程写数据 避免并发问题
     */
    public void run()
    {
        while (!_stop)
        {
            Calendar calendar = Calendar.getInstance();
            if (calendar.get(Calendar.HOUR_OF_DAY) == 2 && calendar.get(Calendar.DAY_OF_YEAR) != day)
            {
                day = calendar.get(Calendar.DAY_OF_YEAR);
                _status = 1;
            }

            if (_status == 0)
            {
                try
                {
                    Thread.sleep(30 * 60 * 1000);
                } catch (Exception e)
                {
                    log.error(e.getMessage(), e);
                }
            } else if (_status == 1) // 开始merge数据
            {
                try
                {
                    _bitCask.merge();
                    log.info("merge 完毕!");
                } catch (Exception e)
                {
                    log.error(e.getMessage(), e);
                } finally
                {
                    _status = 0;
                }
            }
        }
    }

    private static final Comparator<? super File> REVERSE_DATA_FILE_COMPARATOR = new Comparator<File>()
    {

        @Override
        public int compare(File file0, File file1)
        {
            int i0 = BitCaskFile.tstamp(file0);
            int i1 = BitCaskFile.tstamp(file1);

            if (i0 < i1)
                return 1;
            if (i0 == i1)
                return 0;

            return -1;
        }
    };

    static int tstamp()
    {
        return (int) (System.currentTimeMillis() / 1000L);
    }

    public class StoreEvictionListener implements EvictionListener
    {

        @Override
        public void onEviction(Object key, Object value)
        {
            _eviction.incrementAndGet();
        }

    }

    public static class SimpleBitcaskStoreThreadFactory implements ThreadFactory
    {
        public Thread newThread(Runnable runable)
        {
            Thread thread = new Thread(runable);
            thread.setName("SimpleBitcaskStore-pool-thread-" + _threadCounter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        }

        private static AtomicLong _threadCounter = new AtomicLong(0);
    }

    public static boolean deleteDirectory(File path)
    {
        if (path.exists())
        {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++)
            {
                if (files[i].isDirectory())
                {
                    deleteDirectory(files[i]);
                } else
                {
                    files[i].delete();
                }
            }
        }
        return path.delete();
    }

    @Override
    public long getEviction()
    {
        return _eviction.get();
    }

    public void setBlockSize(long size)
    {
        _opts.max_file_size = size;
    }

    @Override
    public void clearCache()
    {
        _eviction.set(0);
        _cache.clear();
    }

    @Override
    public int queueSize()
    {
        return 0;
    }

    @Override
    public void merge()
    {
        _bitCask.merge();
    }

}
