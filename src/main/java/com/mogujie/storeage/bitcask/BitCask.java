package com.mogujie.storeage.bitcask;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.mogujie.storeage.bitcask.BitCaskLock.Type;

public class BitCask
{
    private static final Logger log = Logger.getLogger(BitCask.class);
    private static final ByteString TOMBSTONE = ByteString.copyFromUtf8("bitcask_tombstone");

    /** bc_state */
    File dirname;
    BitCaskFile write_file = BitCaskFile.FRESH_FILE;
    BitCaskLock write_lock;
    ConcurrentHashMap<File, BitCaskFile> read_files = new ConcurrentHashMap<File, BitCaskFile>();
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    ReadLock rLock;
    WriteLock wLock;
    long max_file_size;
    BitCaskOptions opts;
    BitCaskKeyDir keydir;

    private BitCask()
    {
        rLock = rwLock.readLock();
        wLock = rwLock.writeLock();
    }

    public static BitCask open(File dirname, BitCaskOptions opts) throws Exception
    {
        BitCask result = new BitCask();
        BitCaskFile.ensuredir(new File(dirname, "bitcask"));
        if (opts.is_read_write())
        {
            BitCaskLock.delete_stale_lock(Type.WRITE, dirname);
            result.write_file = BitCaskFile.FRESH_FILE;
        }
        result.dirname = dirname;
        BitCaskKeyDir keydir;
        keydir = BitCaskKeyDir.keydir_new(dirname, opts.open_timeout_secs);
        result.keydir = keydir;
        if (!keydir.is_ready())
        {
            File[] files = result.readable_files();
            BitCask.scan_key_files(result, files, keydir);
            keydir.mark_ready();
        }
        result.max_file_size = opts.max_file_size;
        result.dirname = dirname;
        result.opts = opts;
        return result;
    }

    public void close() throws IOException
    {

        // release?
        keydir = null;

        for (BitCaskFile read_file : read_files.values())
        {
            read_file.close();
        }

        read_files.clear();

        if (write_file == null || write_file == BitCaskFile.FRESH_FILE)
        {
            // ok
        } else
        {
            write_file.close();
            write_lock.release();
        }

        BitCaskKeyDir.keydir_close(dirname);

    }

    public Map<File, BitCaskFile> readFiles()
    {
        return read_files;
    }

    public void put(ByteString key, ByteString value) throws IOException
    {
        if (write_file == null)
        {
            throw new IOException("read only");
        }

        wLock.lock();
        try
        {
            switch (write_file.check_write(key, value, max_file_size))
            {
            case WRAP:
            {
                write_file.close_for_writing();
                BitCaskFile last_write_file = write_file;
                BitCaskFile nwf = BitCaskFile.create(dirname);
                write_lock.write_activefile(nwf);

                write_file = nwf;

                if (read_files.get(last_write_file.filename) == null)
                {
                    read_files.put(last_write_file.filename, last_write_file);
                } else
                {
                    log.info("writing bitcaskfile " + last_write_file.filename + " aready reading!");
                    last_write_file.close();
                }
                break;
            }

            case FRESH:
            // time to start our first write file
            {
                BitCaskLock wl = BitCaskLock.acquire(Type.WRITE, dirname);
                BitCaskFile nwf = BitCaskFile.create(dirname);
                wl.write_activefile(nwf);

                this.write_lock = wl;
                this.write_file = nwf;

                break;
            }

            case OK:
                // we're good to go
            }

            BitCaskEntry entry = write_file.write(key, value);
            keydir.put(key, entry);
        } finally
        {
            wLock.unlock();
        }
    }

    public ByteString get(ByteString key) throws IOException
    {
        return get(key, 2);
    }

    private ByteString get(ByteString key, int try_num) throws IOException
    {
        rLock.lock();
        try
        {
            BitCaskEntry entry = keydir.get(key);
            if (entry == null)
            {
                return null;
            }

            if (entry.tstamp < opts.expiry_time())
            {
                return null;
            }

            BitCaskFile file_state = get_filestate(entry.file_id);
            /** merging deleted file between keydir.get and here */
            if (file_state == null)
            {
                Thread.yield();
                return get(key, try_num - 1);
            }

            ByteString[] kv = file_state.read(entry.offset, entry.total_sz);

            if (kv[1].equals(TOMBSTONE))
            {
                return null;
            } else
            {
                return kv[1];
            }
        } finally
        {
            rLock.unlock();
        }
    }

    private BitCaskFile get_filestate(int fileId) throws IOException
    {

        File fname = BitCaskFile.mk_filename(dirname, fileId);
        BitCaskFile f = read_files.get(fname);
        if (f != null)
        {
            return f;
        }

        f = BitCaskFile.open(dirname, fileId);
        read_files.put(fname, f);

        return f;
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

    File[] readable_files()
    {

        final File writing_file = BitCaskLock.read_activefile(Type.WRITE, dirname);
        final File merging_file = BitCaskLock.read_activefile(Type.MERGE, dirname);

        return list_data_files(writing_file, merging_file);
    }

    static Pattern DATA_FILE = Pattern.compile("[0-9]+.bitcask.data");

    private File[] list_data_files(final File writing_file, final File merging_file)
    {
        File[] files = dirname.listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File f)
            {
                if (f == writing_file || f == merging_file)
                    return false;

                return DATA_FILE.matcher(f.getName()).matches();
            }
        });

        Arrays.sort(files, 0, files.length, REVERSE_DATA_FILE_COMPARATOR);

        return files;
    }

    static void scan_key_files(BitCask bitCask, File[] files, final BitCaskKeyDir keydir) throws Exception
    {
        for (File f : files)
        {
            final BitCaskFile file = BitCaskFile.open(f);
            file.fold_keys(new KeyIter<Void>()
            {
                public Void each(ByteString key, int tstamp, long entryPos, int entrySize, Void acc) throws Exception
                {
                    keydir.put(key, new BitCaskEntry(file.file_id, tstamp, entryPos, entrySize));
                    return null;
                }
            }, null);
            bitCask.read_files.put(f, file);
        }
    }

    public String getString(String key) throws IOException
    {
        ByteString val = get(ByteString.copyFromUtf8(key));
        if (val == null)
            return null;
        return val.toStringUtf8();
    }

    public long diskSize()
    {
        return read_files.size() * max_file_size;
    }

    public int size()
    {
        return keydir.size();
    }

    public <T> T fold(final KeyValueIter<T> entryIter, T acc) throws IOException
    {
        final int expiry_time = opts.expiry_time();
        EntryIter<T> iter = new EntryIter<T>()
        {
            public T each(ByteString key, ByteString value, int tstamp, long entryPos, int entrySize, T acc)
            {
                if (tstamp < expiry_time)
                {
                    return acc;
                }
                BitCaskEntry ent = keydir.get(key);
                if (entryPos != ent.offset)
                {
                    return acc;
                }
                if (value.equals(TOMBSTONE))
                {
                    return acc;
                }

                if (ent.tstamp != tstamp)
                {
                    return acc;
                }
                return entryIter.each(key, value, acc);
            }
        };

        BitCaskFile[] files = open_fold_files();
        if (files != null)
        {
            int length = files.length;
            for (int i = 0; i < length; i++)
            {
                acc = files[i].fold(iter, acc);
                log.info("共个" + length + "文件,遍历到第" + (i + 1) + "个! 文件名称:" + files[i].filename.getAbsolutePath());
            }
        }
        for (BitCaskFile bitcaskFile : files)
        {
            bitcaskFile.close();
        }
        return acc;
    }

    public void merge()
    {
        KeyIter<MergeDo> iter = new KeyIter<MergeDo>()
        {
            public MergeDo each(ByteString key, int tstamp, long entry_pos, int entry_size, MergeDo mergDo) throws Exception
            {
                BitCaskEntry ent = keydir.get(key);
                if (ent.tstamp <= tstamp)
                {
                    BitCaskEntry entry = new BitCaskEntry(0, tstamp, entry_pos, entry_size);
                    mergDo.list.add(entry);
                }
                mergDo.total++;
                return mergDo;
            }
        };

        Map<File, MergeDo> mergeMap = new HashMap<File, MergeDo>();
        Map<File, MergeDo> compactMap = new HashMap<File, MergeDo>();

        Set<File> fileSet = read_files.keySet();
        for (File file : fileSet)
        {
            MergeDo mergeDo = new MergeDo();
            BitCaskFile bitCaskFile = read_files.get(file);
            if (bitCaskFile == null)
            {
                continue;
            }
            if (write_file.file_id == bitCaskFile.file_id)
            {
                continue;
            }
            try
            {
                bitCaskFile.fold_keys(iter, mergeDo);
                for (BitCaskEntry bitCaskEntry : mergeDo.list)
                {
                    bitCaskEntry.file_id = bitCaskFile.file_id;
                }
            } catch (Exception e)
            {
                log.error("merge fail" + e.getMessage(), e);
                continue;
            }
            // TODO configure able
            if (mergeDo.needDeleted())
            {
                log.info("bitcask文件" + file.getAbsolutePath() + "包含" + mergeDo.list.size() + "个有效数据 直接删除");
                try
                {
                    read_files.remove(file);
                    bitCaskFile.close();
                    if (bitCaskFile.hasHintfile())
                    {
                        File hitFile = BitCaskFile.hint_filename(file);
                        hitFile.delete();
                    }
                    file.delete();
                } catch (IOException e)
                {
                    log.error(e.getMessage(), e);
                }
            } else if (mergeDo.needCompact())
            {
                log.info("bitcask文件" + file.getAbsolutePath() + "包含" + mergeDo.list.size() + "个有效数据 个数小于1000");
                compactMap.put(file, mergeDo);
            } else if (mergeDo.needMerge())
            {
                log.info("bitcask文件" + file.getAbsolutePath() + "包含的" + mergeDo.total + "个数据" + mergeDo.list.size() + "个有效数据个数小于0.4");
                mergeMap.put(file, mergeDo);
            } else
            {
                log.info("bitcask文件" + file.getAbsolutePath() + "包含的" + mergeDo.total + "个数据" + mergeDo.list.size() + "有效数据个数大于0.4");
            }
        }
        // do merge
        for (Entry<File, MergeDo> mergeEntry : mergeMap.entrySet())
        {
            File file = mergeEntry.getKey();
            MergeDo mergeDo = mergeEntry.getValue();
            List<BitCaskEntry> keys = mergeDo.list;
            BitCaskFile bitCaskFile = read_files.get(file);
            if (bitCaskFile == null)
            {
                continue;
            }
            try
            {
                BitCaskFile mergeBitCaskFile = BitCaskFile.create(dirname);
                Map<ByteString, BitCaskEntry> mergeKeyMap = new HashMap<ByteString, BitCaskEntry>();
                for (BitCaskEntry bitCaskEntry : keys)
                {
                    ByteString[] kv = bitCaskFile.read(bitCaskEntry.offset, bitCaskEntry.total_sz);
                    BitCaskEntry entry = mergeBitCaskFile.write(kv[0], kv[1], bitCaskEntry.tstamp);
                    mergeKeyMap.put(kv[0], entry);
                }
                // lock
                wLock.lock();
                try
                {
                    for (Entry<ByteString, BitCaskEntry> entry : mergeKeyMap.entrySet())
                    {
                        keydir.replace(entry.getKey(), entry.getValue());
                    }
                    File mergeFile = BitCaskFile.mk_filename(dirname, mergeBitCaskFile.file_id);
                    read_files.put(mergeFile, mergeBitCaskFile);
                    read_files.remove(file);
                    bitCaskFile.close();
                    if (bitCaskFile.hasHintfile())
                    {
                        File hitFile = BitCaskFile.hint_filename(file);
                        hitFile.delete();
                    }
                    file.delete();
                    log.info("replace " + bitCaskFile.file_id + " by " + mergeBitCaskFile.file_id);
                } finally
                {
                    wLock.unlock();
                }

            } catch (IOException e)
            {
                log.error(e.getMessage(), e);
            }
        }

        if (compactMap.size() >= 5)
        {
            // lock
            wLock.lock();
            try
            {
                BitCaskFile mergeBitCaskFile = BitCaskFile.create(dirname);
                Map<ByteString, BitCaskEntry> mergeKeyMap = new HashMap<ByteString, BitCaskEntry>();
                for (Entry<File, MergeDo> tempEntry : compactMap.entrySet())
                {
                    MergeDo mergeDo = tempEntry.getValue();
                    BitCaskFile bitCaskFile = read_files.get(tempEntry.getKey());
                    if (bitCaskFile == null)
                    {
                        log.info("bitcaskFile can not found," + tempEntry.getKey().getAbsolutePath());
                        continue;
                    }
                    for (BitCaskEntry bitCaskEntry : mergeDo.list)
                    {
                        ByteString[] kv = bitCaskFile.read(bitCaskEntry.offset, bitCaskEntry.total_sz);
                        BitCaskEntry entry = mergeBitCaskFile.write(kv[0], kv[1], bitCaskEntry.tstamp);
                        mergeKeyMap.put(kv[0], entry);
                    }
                }

                for (Entry<ByteString, BitCaskEntry> entry : mergeKeyMap.entrySet())
                {
                    keydir.replace(entry.getKey(), entry.getValue());
                }
                File compactFile = BitCaskFile.mk_filename(dirname, mergeBitCaskFile.file_id);
                read_files.put(compactFile, mergeBitCaskFile);
                for (Entry<File, MergeDo> tempEntry : compactMap.entrySet())
                {
                    BitCaskFile bitCaskFile = read_files.get(tempEntry.getKey());
                    read_files.remove(tempEntry.getKey());
                    bitCaskFile.close();
                    if (bitCaskFile.hasHintfile())
                    {
                        File hitFile = BitCaskFile.hint_filename(tempEntry.getKey());
                        hitFile.delete();
                    }
                    tempEntry.getKey().delete();
                    log.info("compact " + bitCaskFile.file_id + " to " + mergeBitCaskFile.file_id);
                }
            } catch (Exception e)
            {
                log.error(e.getMessage(), e);
            } finally
            {
                wLock.unlock();
            }
        }
    }

    private BitCaskFile[] open_fold_files()
    {
        File[] files = list_data_files(null, null);
        return open_files(files);
    }

    BitCaskFile[] open_files(File[] files)
    {
        BitCaskFile[] out = new BitCaskFile[files.length];
        for (int i = 0; i < out.length; i++)
        {
            try
            {
                out[i] = BitCaskFile.open(files[i]);
            } catch (IOException e)
            {
                for (int j = 0; j < i; j++)
                {
                    try
                    {
                        out[j].close();
                    } catch (IOException e1)
                    {
                    }
                }

                return null;
            }
        }
        return out;
    }

    private class MergeDo
    {
        public List<BitCaskEntry> list = new ArrayList<BitCaskEntry>();
        public int total = 0;

        public boolean needCompact()
        {
            if (total < 1000)
            {
                return true;
            }
            return false;
        }

        public boolean needDeleted()
        {
            if (list.size() <= 0)
            {
                return true;
            }
            return false;
        }

        public boolean needMerge()
        {
            if (total == 0)
            {
                return false;
            }
            if ((list.size() / (float) total) < 0.4)
            {
                return true;
            }
            return false;
        }
    }

}
