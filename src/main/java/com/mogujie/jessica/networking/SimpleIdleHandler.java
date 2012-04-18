package com.mogujie.jessica.networking;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;

import com.mogujie.jessica.util.Constants;

public class SimpleIdleHandler extends IdleStateAwareChannelHandler
{
    private static final Logger log = Logger.getLogger(Constants.LOG_SEARCH);

    public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) throws Exception
    {
        // just close idle connection
        if (e.getState() == IdleState.ALL_IDLE)
        {
            ChannelFuture future = ctx.getChannel().close();
            future = future.await();
            if (future.isSuccess())
            {
                log.info("close idle connecton: client ip=>" + ctx.getChannel().getRemoteAddress());
            } else
            {
                log.info("failure close idle connecton: client ip=>" + ctx.getChannel().getRemoteAddress());

            }
        }
    }
}
