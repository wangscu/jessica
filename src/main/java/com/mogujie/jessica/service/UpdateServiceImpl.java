package com.mogujie.jessica.service;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import com.mogujie.jessica.index.IndexWriter;
import com.mogujie.jessica.journal.Journal;
import com.mogujie.jessica.scorer.SimpleScorer;
import com.mogujie.jessica.service.thrift.Doc;
import com.mogujie.jessica.service.thrift.ResultCode;
import com.mogujie.jessica.service.thrift.TDocument;
import com.mogujie.jessica.service.thrift.UpdateService.Iface;
import com.mogujie.jessica.service.thrift.UpdateService.Processor;
import com.mogujie.jessica.util.Constants;
import com.mogujie.storeage.SimpleBitcaskStore;

public class UpdateServiceImpl implements Iface
{
    private static final Logger log = Logger.getLogger(Constants.LOG_UPDATE);
    private TServer server;
    private String ip;
    private IndexWriter indexWriter;
    private ExecutorService executor;
    private int port;
    private SimpleBitcaskStore bitcaskStore;
    private Journal journal;

    public UpdateServiceImpl(IndexWriter indexWriter, SimpleBitcaskStore bitcaskStore,Journal journal, String ip, int port)
    {
        this.indexWriter = indexWriter;
        this.bitcaskStore = bitcaskStore;
        this.journal = journal;
        this.ip = ip;
        this.port = port;
        executor = new ThreadPoolExecutor(4, 60, 30000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new UpdateServiceThreadFactory(), new ThreadPoolExecutor.DiscardPolicy());
    }

    public void start()
    {
        log.info("启动更新索引服务!");
        try
        {
            Inet4Address inet4Address = (Inet4Address) Inet4Address.getByName(ip);
            InetSocketAddress socketAddress = new InetSocketAddress(inet4Address, port);
            TNonblockingServerSocket socket = new TNonblockingServerSocket(socketAddress, 30000);
            Processor<UpdateServiceImpl> processor = new Processor<UpdateServiceImpl>(this);

            THsHaServer.Args args = new THsHaServer.Args(socket);
            args.processor(processor);
            args.protocolFactory(new TBinaryProtocol.Factory());
            args.transportFactory(new TFramedTransport.Factory());

            server = new THsHaServer(args);

            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    server.serve();
                }
            });

        } catch (Exception e)
        {
            log.error(e.getMessage(), e);
        }
    }

    public void shutdown()
    {
        server.stop();
    }

    /**
     * @param documents
     *            所有要更新的文档
     * @return resultCode 搜索处理结果
     */
    public ResultCode update(List<TDocument> documents) throws TException
    {
        ResultCode rc = new ResultCode();
        try
        {
            indexWriter.addDocuments(documents);
            List<Doc> docs = new ArrayList<Doc>();
            for (TDocument tDocument : documents)
            {
                docs.add(tDocument.getDoc());
                SimpleScorer.updateData(tDocument.getDoc());
            }
            bitcaskStore.putAll(docs);
            journal.addDocuments(documents);
            rc.setCode(0);
        } catch (Exception e)
        {
            log.error(e.getMessage(), e);
            rc.setCode(1);
            rc.setMsg(e.getMessage());
        }
        return rc;
    }

    public static class UpdateServiceThreadFactory implements ThreadFactory
    {
        public Thread newThread(Runnable runnable)
        {
            Thread thread = new Thread(runnable);
            thread.setName("UpdateService-pool-thread-" + threaCounter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        }

        private final static AtomicLong threaCounter = new AtomicLong(0);
    }
}
