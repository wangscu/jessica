package com.mogujie.jessica.journal;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.protobuf.ByteString;
import com.mogujie.jessica.index.IndexWriter;
import com.mogujie.jessica.scorer.SimpleScorer;
import com.mogujie.jessica.service.thrift.TDocument;
import com.mogujie.jessica.util.Constants;
import com.mogujie.storeage.bitcask.BitCask;
import com.mogujie.storeage.bitcask.BitCaskFile;
import com.mogujie.storeage.bitcask.BitCaskOptions;
import com.mogujie.storeage.bitcask.KeyValueIter;

/**
 * 用于存储索引的日志信息<br/>
 * 当系统重启的时候 可以迅速从日志文件中恢复索引
 * 
 * @author xuanxi
 * 
 */
public class Journal
{
    private static final Logger log = Logger.getLogger(Constants.LOG_ADMIN);
    private static final String SUFFIX = ".journal";
    private static final int DATA_BLOCK_SIZE = 64 * 1024 * 1024;// 64m
    private File journalDir;
    private BitCask bitCask;
    private BitCaskOptions opts;
    private int day = 0;
    private volatile boolean stop = false;
    private int status = 0;
    private long storeVersion = 0;
    private JournalThread thread;
    private ArrayBlockingQueue<List<TDocument>> events = new ArrayBlockingQueue<List<TDocument>>(1000);

    static Pattern DATASTORE_DIR = Pattern.compile("[0-9]+" + SUFFIX);

    public Journal(File dir)
    {
        journalDir = dir;
        if (!journalDir.exists())
        {
            journalDir.mkdirs();
        }
        opts = new BitCaskOptions();
        opts.read_write = true;
        opts.max_file_size = DATA_BLOCK_SIZE;
    }

    /**
     * 写文档
     */
    public void write(TDocument tdocument)
    {
        if (log.isDebugEnabled())
        {
            log.debug(tdocument);
        }

        try
        {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            byte[] bytes = serializer.serialize(tdocument);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bos);
            os.writeLong(tdocument.getObject_id());
            os.flush();
            bos.flush();
            byte[] keyByte = bos.toByteArray();
            os.close();
            bos.close();
            ByteString keyString = ByteString.copyFrom(keyByte);
            ByteString dataString = ByteString.copyFrom(bytes);
            bitCask.put(keyString, dataString);
        } catch (Exception e)
        {
            log.error(e.getMessage(), e);
        }
    }

    public boolean addDocuments(List<TDocument> tdocs) throws Exception
    {
        events.add(tdocs);
        return true;
    }

    /**
     * 通过日志恢复索引
     * 
     * @throws IOException
     */
    public void recover(IndexWriter mIndexWriter) throws IOException
    {
        log.info("start merge jouranl files!");
        bitCask.merge();
        log.info("start recover index from jouranl datas...");
        bitCask.fold(new KeyValueIter<IndexWriter>()
        {
            @Override
            public IndexWriter each(ByteString key, ByteString value, IndexWriter mIndexWriter)
            {
                TDeserializer dserializer = new TDeserializer(new TBinaryProtocol.Factory());
                TDocument tDocument = new TDocument();
                try
                {
                    dserializer.deserialize(tDocument, value.toByteArray());
                    List<TDocument> list = new ArrayList<TDocument>();
                    list.add(tDocument);
                    mIndexWriter.addDocumentDirect(list);
                    for (TDocument td : list)
                    {
                        SimpleScorer.updateData(td.getDoc());
                    }
                } catch (Exception e)
                {
                    log.error(e.getMessage(), e);
                }
                return mIndexWriter;
            }
        }, mIndexWriter);
        log.info("ending recover index operation!");
    }

    public void start()
    {
        try
        {

            File[] files = journalDir.listFiles(new FileFilter()
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
                storeVersion = tstamp();
                currentDir = new File(journalDir, storeVersion + SUFFIX);
                currentDir.mkdirs();
            }

            log.info("启动journal存储服务! 当前数据目录为:" + currentDir.getAbsolutePath());

            bitCask = BitCask.open(currentDir, opts);
            thread = new JournalThread();
            thread.setDaemon(true);
            thread.start();
        } catch (Exception e)
        {
            log.error(e.getMessage(), e);
        }

    }

    public void shutdown()
    {
        try
        {
            stop = true;
            bitCask.close();
        } catch (IOException e)
        {
            log.error(e.getMessage(), e);
        }
    }

    private class JournalThread extends Thread
    {
        /**
         * 异步更新接口 保证只有一个线程写数据 避免并发问题
         */
        public void run()
        {
            while (!stop)
            {
                Calendar calendar = Calendar.getInstance();
                if (calendar.get(Calendar.HOUR_OF_DAY) == 3 && calendar.get(Calendar.DAY_OF_YEAR) != day)
                {
                    day = calendar.get(Calendar.DAY_OF_YEAR);
                    status = 1;
                }

                if (status == 0)
                {
                    try
                    {
                        List<TDocument> tdocs = Journal.this.events.take();
                        for (TDocument tDocument : tdocs)
                        {
                            write(tDocument);
                        }
                    } catch (Exception e)
                    {
                        log.error(e.getMessage(), e);
                    }
                } else if (status == 1) // 开始merge数据
                {
                    try
                    {
                        bitCask.merge();
                        log.info("merge 完毕!");
                    } catch (Exception e)
                    {
                        log.error(e.getMessage(), e);
                    } finally
                    {
                        status = 0;
                    }
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
}
