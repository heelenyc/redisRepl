package redis.repl.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.repl.api.ReplyStatus;
import redis.repl.context.ReplyContext;

/**
 * 没有拆包，不知道后面的状态，无法精确增加，弃用
 * @author yicheng
 * @since 2016年10月19日
 * 
 */
public class ByteCounterHandler extends ChannelInboundHandlerAdapter {

    private Logger logger = LoggerFactory.getLogger(ByteCounterHandler.class);

    private ReplyContext replyContext;

    /**
     * 
     */
    public ByteCounterHandler(ReplyContext replyContext) {
        this.replyContext = replyContext;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        replyContext.setLastiotimestmap(System.currentTimeMillis());
        if (replyContext.getStatus() == ReplyStatus.ONLINE_MODE) {
            int inc = ((ByteBuf) msg).readableBytes();
            logger.info("inc offset : " + inc);
            replyContext.incOffset(inc);
        }
        super.channelRead(ctx, msg);
    }
}
