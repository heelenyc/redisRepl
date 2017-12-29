package redis.repl;

import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.repl.api.ReplyStatus;
import redis.repl.cmd.CommonCmdUtil;
import redis.repl.coder.RedisMsgDecoder;
import redis.repl.coder.RedisMsgEncoder;
import redis.repl.context.ReplyContext;
import redis.repl.handler.ReplicationHandler;
import redis.repl.msg.redis.ArrayMsg;

/**
 * @author yicheng
 * @since 2016年10月19日
 * 
 */
public class SlaveClient {

    private static Logger logger = LoggerFactory.getLogger(SlaveClient.class);

    private static ReplyContext replyContext;

    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();

        replyContext = new ReplyContext();// test

        b.group(group).channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true).handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline channelPipeline = ch.pipeline();
                // channelPipeline.addLast("bytecounter", new
                // ByteCounterHandler(replyContext));
                channelPipeline.addLast("decoder", new RedisMsgDecoder(replyContext));
                channelPipeline.addLast("encoder", new RedisMsgEncoder());
                channelPipeline.addLast("ReplicationHandler", new ReplicationHandler(replyContext));
            }
        });

        ChannelFuture f = b.connect("127.0.0.1", 6379).sync();
        logger.info("connected finished!");
        addAckSchedule(f.channel());
        // will block this thead
        f.channel().closeFuture().sync();
        logger.info("connect close!");

    }
    
	public static void addAckSchedule(final Channel ch){
        // 定时回复 runid 和 offset
        if (replyContext.getIsAckStarted() == false) {
            synchronized (replyContext.getIsAckStarted()) {
                if (replyContext.getIsAckStarted() == false) {
                    ch.eventLoop().scheduleAtFixedRate(new Runnable() {
                    	private long loopNum = 0l;
                        @Override
                        public void run() {
                        	loopNum++;
                        	if (replyContext.getStatus().equals(ReplyStatus.ONLINE_MODE)) {
//                        		if (loopNum == 30) {
//                        			ArrayMsg ack = CommonCmdUtil.newReplACKCmd(100l);
//                                    logger.info("send REPLCONF offset: 0");
//                                    ch.writeAndFlush(ack);
//								} else if(loopNum <= 30) {
								ArrayMsg ack = CommonCmdUtil.newReplACKCmd(replyContext.getOffset());
								// logger.info("send REPLCONF offset: " + replyContext.getOffset());
								ch.writeAndFlush(ack);
//								}
                        		
							} else if (loopNum % 5 == 0 && replyContext.getStatus().equals(ReplyStatus.PARSE_RDB)) {
								logger.info("send \n to master !");
								ByteBufAllocator alloc = ch.alloc();
						        ByteBuf buf = alloc.buffer(1);
						        buf.writeByte('\n');
                                ch.writeAndFlush(buf);
							}
                        }
                    }, 1, 1, TimeUnit.SECONDS);
                    replyContext.setIsAckStarted(true);
                }
            }
        }
    }

}
