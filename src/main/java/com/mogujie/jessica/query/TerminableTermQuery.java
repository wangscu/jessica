package com.mogujie.jessica.query;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

import com.mogujie.jessica.util.TerminalbleSkippableIterator;

/**
 * 一个可以中断的term查询
 * 
 * @author xuanxi
 * 
 */
public class TerminableTermQuery extends TermQuery implements Serializable
{
    private final static Logger logger = Logger.getLogger(TerminableTermQuery.class);
    // 每秒轮训一次 最长超时50ms 第二轮
    private static HashedWheelTimer timer = new HashedWheelTimer(1, TimeUnit.MILLISECONDS, 50);

    // 查询开始的时候开始计算时间 ticktock!!!! ticktock!!!!
    public static <E> void ticktock(final TerminableTermQuery query, final TerminalbleSkippableIterator<E> iterator)
    {
        timer.newTimeout(new TimerTask()
        {
            @Override
            public void run(Timeout arg0) throws Exception
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("查询被中断:" + query + "被中断, 耗时:" + query.getQueryTime() + "ms!");
                }
                iterator.terminal();
            }
        }, query.queryTime, TimeUnit.MILLISECONDS);
    }

    private static final long serialVersionUID = 1L;
    private int queryTime;// 最长执行时间 ms

    public TerminableTermQuery(final String field, final String term, final int queryTime)
    {
        super(field, term);

        this.queryTime = queryTime;
    }

    public int getQueryTime()
    {
        return queryTime;
    }

    public void setQueryTime(int queryTime)
    {
        this.queryTime = queryTime;
    }

    @Override
    public QueryNode duplicate()
    {
        return new TerminableTermQuery(field, term, queryTime);
    }

}
