package redis.repl.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.repl.SlaveClient;
import redis.repl.api.AbstractRedisMsg;
import redis.repl.api.ReplyStatus;
import redis.repl.cmd.CommonCmdUtil;
import redis.repl.context.ReplyContext;
import redis.repl.msg.SimpleStringMsg;

/**
 * @author yicheng
 * @since 2016年10月19日
 * 
 */
public class ReplicationHandler extends SimpleChannelInboundHandler<AbstractRedisMsg<?>> {

    private Logger logger = LoggerFactory.getLogger(ReplicationHandler.class);

    private ReplyContext replyContext;

    /**
     * 
     */
    public ReplicationHandler(ReplyContext replyContext) {
        this.replyContext = replyContext;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // super.channelActive(ctx);
        // 连接成功之后 发送一个ping操作
        if (replyContext.getStatus() == ReplyStatus.TO_SEND_PING) {
            ctx.channel().writeAndFlush(CommonCmdUtil.PING, ctx.newPromise().addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    logger.info("finsih send PING");
                    replyContext.setStatus(ReplyStatus.FINISH_SEND_PING);
                }
            }));
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    /**
     * 从下一个字节开始拷贝
     * 
     * @return
     */
    private long getPsyncOffset() {
        if (replyContext.getOffset() <= 0) {
            return replyContext.getOffset();
        } else {
            return replyContext.getOffset() + 1;
        }
        // return replyContext.getOffset();
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, AbstractRedisMsg<?> msg) throws Exception {

        if (replyContext.getStatus() == ReplyStatus.FINISH_SEND_PING) {
            // 收到pong回应之后 发psync操作
            if (CommonCmdUtil.PONG.equals(msg)) {
                logger.info("recv : " + msg);
                replyContext.setStatus(ReplyStatus.TO_SEND_PSYNC);
                // 发送成功后修改状态
                ctx.channel().writeAndFlush(CommonCmdUtil.newPyncCmd(replyContext.getRunID(), getPsyncOffset()), ctx.newPromise().addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        logger.info("finsih send PSYNC");
                        replyContext.setStatus(ReplyStatus.FINISH_SEND_PSYNC);
                    }
                }));
            } else {
                logger.error("unexpected msg : " + msg);
            }
        } else if (replyContext.getStatus() == ReplyStatus.FINISH_SEND_PSYNC) {
            if (CommonCmdUtil.isFullSyncmd(msg)) {
                // 如果server回复是full sync ，准备rdb
                logger.info("recv : " + msg);
                SimpleStringMsg cmd = (SimpleStringMsg) msg;
                String[] item = cmd.data().split(" ");
                replyContext.setRDB_runID(item[1]);
                replyContext.setRDB_offset(Long.valueOf(item[2]));
                replyContext.setStatus(ReplyStatus.TO_TRANSFER_RDB);

            } else if (CommonCmdUtil.isContinueCmd(msg)) {
                // 如果server 回复是 continue，开始增量模式
                logger.info("recv :" + msg + "  bytes :" + msg.getOffsetSize());
                replyContext.setStatus(ReplyStatus.ONLINE_MODE);
                SlaveClient.addAckSchedule(ctx.channel());
            } else {
                logger.error("unexpected msg : " + msg);
            }
        } else if (replyContext.getStatus() == ReplyStatus.ONLINE_MODE) {
            logger.info("online : " + msg + "  bytes :" + (msg.getOffsetSize() != 14 ? "===== msg.getByteSize()" : ""));
            replyContext.incOffset(msg.getOffsetSize());
            if (CommonCmdUtil.PING.equals(msg)) {
                // 回 pong 操作
                // ctx.writeAndFlush(CommonCmdUtil.PONG);
            }
        }
    }
}
