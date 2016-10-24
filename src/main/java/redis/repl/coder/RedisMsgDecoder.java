package redis.repl.coder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.repl.api.AbstractRedisMsg;
import redis.repl.api.ReplyStatus;
import redis.repl.cmd.CommonCmdUtil;
import redis.repl.context.ReplyContext;
import redis.repl.msg.ArrayMsg;
import redis.repl.msg.BulkMsg;
import redis.repl.msg.ErrorMsg;
import redis.repl.msg.IntegerMsg;
import redis.repl.msg.SimpleStringMsg;

/**
 * 客户端读回应，server读命令，但是都是消息
 * 
 * @author yicheng
 * @since 2016年1月11日
 * 
 */
public class RedisMsgDecoder extends ReplayingDecoder<RedisProtocolState> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ReplyContext replyContext;

    /** Decoded command and arguments */
    private RedisCommand redisCommand;

    private int argSize;
    private int argIndex = 0;
    private int commandBytes = 0;

    public RedisMsgDecoder(ReplyContext replyContext) {
        super(RedisProtocolState.TO_READ_PREFIX);
        this.replyContext = replyContext;
    }

    private byte getChar(byte b) {
        // return (char) (((0 & 0xFF) << 8) | ( b & 0xFF));
        return b;
    }

    /**
     * Decode in block-io style, rather than nio. because reps protocol has a
     * dynamic body len
     */
    @Override
    protected void decode(final ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        if (replyContext.getStatus() != ReplyStatus.TO_TRANSFER_RDB) {
            // 非rdb传输的时候 解析命令
            switch (state()) {
            case TO_READ_PREFIX:
                byte prefix = in.readByte();
                // 开始读命令
                // TODO 增量复制的时候这个地方有问题，特别是offset不是从master出来的值。
                commandBytes = commandBytes + 1; 

                if (prefix == '*') {
                    checkpoint(RedisProtocolState.TO_READ_ARRAY_NUM);
                } else if (prefix == '+') {
                    checkpoint(RedisProtocolState.TO_READ_SIMPLESTR);
                } else if (prefix == '-') {
                    checkpoint(RedisProtocolState.TO_READ_ERROR);
                } else if (prefix == ':') {
                    checkpoint(RedisProtocolState.TO_READ_INTEGER);
                } else if (prefix == '$') {
                    checkpoint(RedisProtocolState.TO_READ_BULK);
                    // } else if (prefix == '\r') {
                    // // donothing
                    // logger.info("unexpected prefix : " + getChar(prefix));
                    // } else if (prefix == '\n') {
                    // // donothing
                    // logger.info("unexpected prefix : " + getChar(prefix));
                } else {
                    // 理论不会到这里
                    logger.error("unexpected prefix for redis request : " + getChar(prefix));
                    // ctx.close();
                }
                break;
            case TO_READ_SIMPLESTR:
                String simpleString = readStringAndSkipCRCF(in);
                // 解析完成
                checkpoint(RedisProtocolState.TO_READ_PREFIX);
                sendCmdToHandler(out, new SimpleStringMsg(simpleString));
                break;
            case TO_READ_ERROR:
                String error = readStringAndSkipCRCF(in);
                // 解析完成
                checkpoint(RedisProtocolState.TO_READ_PREFIX);
                sendCmdToHandler(out, new ErrorMsg(error));
                break;
            case TO_READ_BULK:
                int len = readIntAndSkipCRCF(in);
                // 解析完成
                byte[] bulkByte = new byte[len];
                in.readBytes(bulkByte);
                commandBytes = commandBytes + len;
                in.skipBytes(2);
                commandBytes = commandBytes + 2;
                // 读取完成
                String bulk = new String(bulkByte);
                checkpoint(RedisProtocolState.TO_READ_PREFIX);
                sendCmdToHandler(out, new BulkMsg(bulk));
                break;
            case TO_READ_INTEGER:
                int i = readIntAndSkipCRCF(in);
                // 解析完成
                checkpoint(RedisProtocolState.TO_READ_PREFIX);
                sendCmdToHandler(out, new IntegerMsg(i));
                break;
            case TO_READ_ARRAY_NUM:
                decodeNumOfArgs(in);
                checkpoint(RedisProtocolState.TO_READ_ARRAY_ARGS);
                break;
            case TO_READ_ARRAY_ARGS:
                while (argIndex < argSize) {
                    if (in.readByte() == '$') {
                        commandBytes = commandBytes + 1;
                        int lenOfBulkStr = readIntAndSkipCRCF(in);
                        // logger.info("RedisCommandDecoder LenOfBulkStr[" +
                        // argIndex + "]: " + lenOfBulkStr);

                        byte[] dest = new byte[lenOfBulkStr];
                        in.readBytes(dest);
                        commandBytes = commandBytes + lenOfBulkStr;
                        // Skip CRLF(\r\n)
                        in.skipBytes(2);
                        commandBytes = commandBytes + 2;
                        // 这次参数读取完成，修改内部状态变量
                        if (argIndex == 0) {
                            // action
                            redisCommand = new RedisCommand(new String(dest));
                        } else {
                            redisCommand.getArgList().add(dest);
                        }
                        argIndex++;
                        // 内部状态变化后 要移动读指针, 疑似bug, no 指定了readbyte的长度
                        checkpoint();

                    } else {
                        throw new IllegalStateException("Invalid argument");
                    }
                }
                // 解析完成
                checkpoint(RedisProtocolState.TO_READ_PREFIX);
                if (isComplete()) {
                    sendCmdToHandler(out, redisCommand.toArrayMsg());
                    clean();
                } else {
                    clean();
                    throw new IllegalStateException("decode command failed : " + redisCommand + ", redisArgSize : " + argSize + ", cmd.args.size() : " + redisCommand.getArgList().size());
                }
                break;
            default:
                throw new IllegalStateException("invalide state default!");
            }
        } else {
            // 传输rdb
            // Read RDB size. Master send '$13123123\r\n'
            if (in.readByte() != '$') {
                throw new Exception("invalid rdb size prefix!");
            }

            int len = readIntAndSkipCRCF(in);
            byte[] rdbByes = new byte[len];
            in.readBytes(rdbByes);
            // 异步线程 解析 rdb
            parseRDB(rdbByes);

            // 重回 命令行模式
            replyContext.setStatus(ReplyStatus.ONLINE_MODE);
            checkpoint(RedisProtocolState.TO_READ_PREFIX);
            // 清理rdb命令的字节计数，避免错误
            commandBytes = 0;

            // 定时回复 runid 和 offset
            if (replyContext.getIsAckStarted() == false) {
                synchronized (replyContext.getIsAckStarted()) {
                    if (replyContext.getIsAckStarted() == false) {
                        ctx.channel().eventLoop().scheduleAtFixedRate(new Runnable() {
                            @Override
                            public void run() {
                                ArrayMsg ack = CommonCmdUtil.newReplACKCmd(replyContext.getOffset());
                                // logger.info("send ack : " + ack);
                                ctx.channel().writeAndFlush(ack);
                                // ctx.writeAndFlush(ack); // no!
                            }
                        }, 1, 1, TimeUnit.SECONDS);
                        replyContext.setIsAckStarted(true);
                    }
                }

            }
        }
    }

    /**
     * 解析rdb不能阻塞io
     * 
     * @param rdbByes
     */
    private void parseRDB(final byte[] rdbByes) {
        new Thread() {
            @Override
            public void run() {
                logger.info("parse RDB length : " + rdbByes.length);
            }
        }.start();
    }

    private void decodeNumOfArgs(ByteBuf in) {
        // Ignore negative case
        argSize = readIntAndSkipCRCF(in);
        logger.debug("RedisCommandDecoder NumOfArgs: " + argSize);
    }

    /**
     * cmds != null means header decode complete arg > 0 means arguments decode
     * has begun arg == cmds.length means complete!
     */
    private boolean isComplete() {
        return redisCommand != null && redisCommand.getAction() != null && !"".equals(redisCommand.getAction().trim()) && redisCommand.getArgList().size() == argSize - 1;
    }

    private void sendCmdToHandler(List<Object> out, AbstractRedisMsg<?> msg) {
        // logger.info("RedisCommandDecoder: Send command to next handler , cmd : "
        // + JsonUtils.toJSON(cmd));
        msg.setByteSize(commandBytes);
        out.add(msg);
        commandBytes = 0;
    }

    /**
     * 清楚内部变量状态值
     */
    private void clean() {
        this.redisCommand = null;
        this.argSize = 0;
        this.argIndex = 0;
        this.commandBytes = 0;
    }

    /**
     * 读取字符型的int值，包括结尾的 \r\n
     * 
     * @param in
     * @return
     */
    private int readIntAndSkipCRCF(ByteBuf in) {
        int integer = 0;
        char c;
        while ((c = (char) in.readByte()) != '\r') {
            commandBytes = commandBytes + 1;
            integer = (integer * 10) + (c - '0');
        }
        // skip \r
        commandBytes = commandBytes + 1;
        // skip \n
        if (in.readByte() != '\n') {
            commandBytes = commandBytes + 1;
            throw new IllegalStateException("Invalid number");
        } else {
            commandBytes = commandBytes + 1;
        }
        return integer;
    }

    private String readStringAndSkipCRCF(ByteBuf in) {
        StringBuffer sb = new StringBuffer();
        char c;
        while ((c = (char) in.readByte()) != '\r') {
            sb.append(c);
            commandBytes = commandBytes + 1;
        }
        // skip \r
        commandBytes = commandBytes + 1;
        // skip \n
        if (in.readByte() != '\n') {
            commandBytes = commandBytes + 1;
            throw new IllegalStateException("Invalid number");
        } else {
            commandBytes = commandBytes + 1;
        }

        return sb.toString();
    }

}
