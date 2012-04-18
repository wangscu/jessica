package com.mogujie.jessica.networking;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;

import com.mogujie.jessica.util.Constants;

/**
 * @project:杭州卷瓜网络有限公司搜索引擎
 * @date:2011-10-15
 * @author:xuanxi
 * @和thrift netty server
 * 
 */
public class SimpleServer
{
    private static final Logger log = Logger.getLogger(Constants.LOG_SEARCH);
    private TProcessor processor;
    private ChannelFactory factory;
    private String ip;
    private int port;
    private ServerBootstrap bootstrap;
    private SimpleServerHandler handler;
    private SimplePipelineFactory pipleFactory;
    private Channel channel;

    public SimpleServer(TProcessor processor, String ip, int port)
    {
        this.processor = processor;
        this.factory = new OioServerSocketChannelFactory(Executors.newFixedThreadPool(1), Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2));
        this.ip = ip;
        this.port = port;

        this.handler = new SimpleServerHandler(processor, new TBinaryProtocol.Factory());
        this.pipleFactory = new SimplePipelineFactory(handler);
        this.bootstrap = new ServerBootstrap(factory);
    }

    public void start()
    {

        bootstrap.setPipelineFactory(this.pipleFactory);
        bootstrap.setOption("child.tcpNoDelay", true);
        // bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.sendBufferSize", 81920);
        bootstrap.setOption("child.receiveBufferSize", 81920);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("backlog", 512);

        Inet4Address inet4Address;
        try
        {
            inet4Address = (Inet4Address) Inet4Address.getByName(ip);
        } catch (UnknownHostException e)
        {
            log.error(e.getMessage(), e);
            return;
        }
        InetSocketAddress socketAddress = new InetSocketAddress(inet4Address, port);
        channel = bootstrap.bind(socketAddress);
    }

    public void shutdonw()
    {
        channel.close();
    }

}
