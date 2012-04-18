package com.mogujie.jessica.index;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.mogujie.jessica.service.thrift.TDocument;

public class IndexWriter
{
    private AtomicBoolean updateAble = new AtomicBoolean(true);
    private ArrayBlockingQueue<List<TDocument>> events = new ArrayBlockingQueue<List<TDocument>>(1000);
    private IndexWriterThread thread = new IndexWriterThread();
    private volatile boolean isEnd = false;
    private InvertedIndexer indexer;

    public IndexWriter()
    {
        indexer = new InvertedIndexer();
    }

    public void start()
    {
        thread.start();
    }

    public void shutdown()
    {
        events.add(new ArrayList<TDocument>());
        isEnd = true;
    }

    public InvertedIndexer getIndexer()
    {
        return indexer;
    }

    public boolean addDocuments(List<TDocument> tdocs) throws Exception
    {
        events.add(tdocs);
        return true;
    }

    public void addDocumentDirect(List<TDocument> tdocs)
    {
        for (TDocument tDocument : tdocs)
        {
            indexer.add(tDocument);
        }
    }

    public void triggerCompact()
    {
        events.add(new ArrayList<TDocument>());
        updateAble.compareAndSet(true, false);
    }

    /**
     * FXIME<br/>
     * 压缩系统所占内存 将已经删除的docId 并紧缩docid的序号<br/>
     * 在这个过程中不允许系统进行文档更新删除操作<br/>
     */
    public void compact()
    {
        indexer = new InvertedIndexer(indexer);
    }

    private class IndexWriterThread extends Thread
    {

        private final Logger logger = Logger.getLogger(IndexWriterThread.class);
        private int day = 0;

        public IndexWriterThread()
        {
            super("IndexWriterThread");
            setDaemon(true);
            this.setUncaughtExceptionHandler(exceptionHandler);
        }

        private Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread thread, Throwable t)
            {
                logger.error(thread.getName() + " is abruptly terminated", t);
            }
        };

        @Override
        public void run()
        {
            while (!isEnd)
            {
                // 每天2点强制合并一次
                Calendar calendar = Calendar.getInstance();
                if (calendar.get(Calendar.HOUR_OF_DAY) == 2 && calendar.get(Calendar.DAY_OF_YEAR) != day)
                {
                    day = calendar.get(Calendar.DAY_OF_YEAR);
                    updateAble.compareAndSet(true, false);
                }
                try
                {
                    if (updateAble.get())
                    {
                        List<TDocument> tdocs = IndexWriter.this.events.take();
                        addDocumentDirect(tdocs);
                    } else
                    {
                        compact();
                    }
                } catch (Exception e)
                {
                    logger.error(e.getMessage(), e);
                }
            }
        }

    }
}
