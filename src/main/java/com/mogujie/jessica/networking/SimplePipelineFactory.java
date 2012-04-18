package com.mogujie.jessica.networking;

import static org.jboss.netty.channel.Channels.pipeline;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.execution.ExecutionHandler;

import com.mogujie.jessica.util.Constants;

/**
 * @project:杭州卷瓜网络有限公司搜索引擎
 * @date:2011-10-15
 * @author:xuanxi
 * @和thrift netty server
 * 
 */
public class SimplePipelineFactory implements ChannelPipelineFactory
{
    private static final Logger logger = Logger.getLogger(Constants.LOG_SEARCH);
    private SimpleServerHandler handler;
    private int maxFrameSize = 512 * 1024;
    private Executor executor;
    private ExecutionHandler executionHandler;

    // private Timer timer = new HashedWheelTimer();

    public SimplePipelineFactory(SimpleServerHandler handler)
    {
        this.handler = handler;
        this.executor = new ThreadPoolExecutor(60, 600, 30000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory()
        {
            @Override
            public Thread newThread(Runnable runnable)
            {
                Thread thread = new Thread(runnable);
                thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler()
                {
                    @Override
                    public void uncaughtException(Thread thread, Throwable e)
                    {
                        logger.error("thread:" + thread.getName() + " uncaughtException:" + e.getMessage(), e);
                    }
                });
                thread.setName("worker-pool-thread-" + threaCounter.incrementAndGet());
                thread.setDaemon(false);
                return thread;
            }
        }, new ThreadPoolExecutor.DiscardPolicy());
        this.executionHandler = new ExecutionHandler(executor);
    }

    public SimplePipelineFactory(SimpleServerHandler handler, int maxFrameSize)
    {
        this(handler);
        this.maxFrameSize = maxFrameSize;
    }

    // TODO 超时处理
    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = pipeline();
        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        // pipeline.addLast("timeout", new IdleStateHandler(timer, 30, 30, 30));
        pipeline.addLast("threadpool", executionHandler);
        // pipeline.addLast("idleHandler", new SimpleIdleHandler());
        pipeline.addLast("thriftHandler", handler);
        return pipeline;
    }

    private final static AtomicLong threaCounter = new AtomicLong(0);
}