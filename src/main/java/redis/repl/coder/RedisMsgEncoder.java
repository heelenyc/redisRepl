package redis.repl.coder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.repl.api.AbstractRedisMsg;

/**
 * @author yicheng
 * @since 2016年1月11日
 *
 */
public class RedisMsgEncoder extends MessageToByteEncoder<AbstractRedisMsg<?>> {
    
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void encode(ChannelHandlerContext ctx, AbstractRedisMsg<?> msg, ByteBuf out) throws Exception {
        logger.debug("RedisMsgEncoder: " + msg);
        msg.write(out);
    }

}
