package redis.repl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.repl.coder.RedisMsgDecoder;
import redis.repl.coder.RedisMsgEncoder;
import redis.repl.context.ReplyContext;
import redis.repl.handler.ReplicationHandler;

/**
 * @author yicheng
 * @since 2016年10月19日
 * 
 */
public class SlaveClient {

    private static Logger logger = LoggerFactory.getLogger(SlaveClient.class);

    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();

        final ReplyContext replyContext = new ReplyContext();

        b.group(group).channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true).handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline channelPipeline = ch.pipeline();
                // channelPipeline.addLast("bytecounter", new ByteCounterHandler(replyContext));
                channelPipeline.addLast("decoder", new RedisMsgDecoder(replyContext));
                channelPipeline.addLast("encoder", new RedisMsgEncoder());
                channelPipeline.addLast("ReplicationHandler", new ReplicationHandler(replyContext));
            }
        });

        ChannelFuture f = b.connect("127.0.0.1", 6379).sync();
        logger.info("connected finished!");
        // will block this thead
        f.channel().closeFuture().sync();
        logger.info("connect close!");
        
    }

}
