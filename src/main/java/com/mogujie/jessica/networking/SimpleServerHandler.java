package com.mogujie.jessica.networking;

import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.mogujie.jessica.util.Constants;

/**
 * @project:杭州卷瓜网络有限公司搜索引擎
 * @date:2011-10-15
 * @author:xuanxi
 * @和thrift netty server
 * 
 */
public class SimpleServerHandler extends SimpleChannelUpstreamHandler
{
    private static final Logger log = Logger.getLogger(Constants.LOG_SEARCH);
    private TProcessor processor;
    private TProtocolFactory protocolFactory;
    private int responseSize = 81920;

    public SimpleServerHandler(TProcessor processor, TProtocolFactory protocolFactory)
    {
        this.processor = processor;
        this.protocolFactory = protocolFactory;
    }

    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
    {
        long sTime = System.currentTimeMillis();
        ChannelBuffer input;
        input = (ChannelBuffer) e.getMessage();
        ChannelBuffer output = ChannelBuffers.dynamicBuffer(responseSize);
        TProtocol protocol = protocolFactory.getProtocol(new SimpleTransport(input, output));
        processor.process(protocol, protocol);
        long eTime = System.currentTimeMillis();
        log.info("结果大小:" + output.readableBytes() + ",耗时:" + (eTime - sTime) + "ms");
        e.getChannel().write(output);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
    {
        if (this == ctx.getPipeline().getLast())
        {
            log.warn("EXCEPTION, please implement " + getClass().getName() + ".exceptionCaught() for proper handling.", e.getCause());
        }
    }
}
