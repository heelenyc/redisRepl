package redis.repl.coder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import net.whitbeck.rdbparser.Aux;
import net.whitbeck.rdbparser.DbSelect;
import net.whitbeck.rdbparser.Entry;
import net.whitbeck.rdbparser.EntryType;
import net.whitbeck.rdbparser.KeyValuePair;
import net.whitbeck.rdbparser.RdbParser;
import net.whitbeck.rdbparser.ResizeDb;
import redis.repl.api.AbstractMsg;
import redis.repl.api.ReplyStatus;
import redis.repl.context.ReplyContext;
import redis.repl.msg.UnexpectedMsg;
import redis.repl.msg.redis.BulkMsg;
import redis.repl.msg.redis.ErrorMsg;
import redis.repl.msg.redis.IntegerMsg;
import redis.repl.msg.redis.SimpleStringMsg;

/**
 * 客户端读回应，server读命令，但是都是消息
 * 
 * @author yicheng
 * @since 2016年1月11日
 * 
 */
public class RedisMsgDecoder extends ReplayingDecoder<RedisProtocolState> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final int RDB_READ_BLOCK = 16 * 1024; // 一次读取16k byte
    private static byte[] RDB_READ_BLOCK_BUFFER = new byte[RDB_READ_BLOCK];
    
    private ReplyContext replyContext;

    // used to crontrol receive rdb file
    private String rdbFileName;
    // private Thread rdbThread;
    private RandomAccessFile file;
    private boolean isReadytoReceive = false;
    private int rdbSize;
    private int readedRdbSize;

    /** Decoded command and arguments */
    private RedisCommand redisCommand;

    private int argSize;
    private int argIndex = 0;

    public RedisMsgDecoder(ReplyContext replyContext) {
        super(RedisProtocolState.TO_READ_PREFIX);
        this.replyContext = replyContext;
    }

//    private char getChar(byte b) {
//        return (char) (((0 & 0xFF) << 8) | (b & 0xFF));
//        // return b;
//    }

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
                } else {
                    // 理论不会到这里
                    out.add(new UnexpectedMsg(prefix));
                    checkpoint(); // 更新读标记 ，避免重复读错误的字符
                    // 注意
                    // 增量复制的时候这个地方有问题，特别是offset不是从master出来的值。错误的字符应该也算上,移到后面去处理
                    // if (replyContext.getStatus() ==
                    // ReplyStatus.FINISH_SEND_PSYNC) {
                    // 这个阶段发送的字节数应该计入offset
                    // replyContext.incOffset(1);
                    // }
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
                in.skipBytes(2);
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
                        int lenOfBulkStr = readIntAndSkipCRCF(in);
                        // logger.info("RedisCommandDecoder LenOfBulkStr[" +
                        // argIndex + "]: " + lenOfBulkStr);

                        byte[] dest = new byte[lenOfBulkStr];
                        in.readBytes(dest);
                        // Skip CRLF(\r\n)
                        in.skipBytes(2);
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
            // replyContext.getStatus() == ReplyStatus.TO_TRANSFER_RDB
            // 传输rdb
            // Read RDB size. Master send '$13123123\r\n'
            if (isReadytoReceive == false) {
                // 开始接收
                if (in.readByte() != '$') {
                    throw new Exception("invalid rdb size prefix!");
                }

                rdbSize = readIntAndSkipCRCF(in);
                logger.info("begin to receive RDB file, size : {}", rdbSize);
                // byte[] rdbByes = new byte[len];
                rdbFileName = String.format("temp-%s.rdb", System.currentTimeMillis());
                file = new RandomAccessFile(rdbFileName, "rw");
                isReadytoReceive = true;
                checkpoint();
            }
            byte[] readbuf ;
            while (readedRdbSize < rdbSize) {
                int toReadSize = rdbSize - readedRdbSize; // 剩余要读取的
                //  这个地方有问题，没法.ReplayingDecoderBuffer.readableBytes() 不准确，如果这个rdb文件较大，可能多次重复读, 所以拆分
//                if (in.readableBytes() < toReadSize) {
//                    toReadSize = in.readableBytes();
//                }
                if (toReadSize > RDB_READ_BLOCK) {
                	toReadSize = RDB_READ_BLOCK;
                	readbuf = RDB_READ_BLOCK_BUFFER; 
				} else {
					readbuf = new byte[toReadSize]; 
				}
                // 非得先读出来。。。
                in.readBytes(readbuf);
                file.write(readbuf);
                readedRdbSize += toReadSize;
                checkpoint();
            }
            // 异步线程 解析 rdb
            replyContext.setStatus(ReplyStatus.PARSE_RDB);
            // rdb解析前停止读取
            ctx.channel().config().setAutoRead(false);
            parseRDB(ctx.channel());
            checkpoint(RedisProtocolState.TO_READ_PREFIX);
        }
    }
    
    @SuppressWarnings("unused")
	private String prettyString(List<byte[]> list){
        StringBuffer sb = new StringBuffer();
        sb.append('[');
        for (byte[] b:list) {
            sb.append(new String(b));
            sb.append(',');
        }
        sb.setCharAt(sb.length()-1,']');
        return sb.toString();
    }

    /**
     * 解析rdb不能阻塞io
     * 
     * @param rdbByes
     */
    private void parseRDB(final Channel ch) {
        new Thread() {
            @Override
            public void run() {
                RdbParser parser = null;
                try {
                    logger.info("begin parse RDB length : " + file.length());

                    parser = new RdbParser(rdbFileName);

                    Entry e = null;
                    while ((e = parser.readNext()) != null) {
                        if (e.getType() == EntryType.DB_SELECT) {
                            logger.info("DB_SELECT: processing DB: {}", ((DbSelect) e).getId());
                        } else if (e.getType() == EntryType.EOF) {
                        	logger.info("EOF: finish parsing RDB");
                        } else if (e.getType() == EntryType.KEY_VALUE_PAIR) {
                            KeyValuePair kvp = (KeyValuePair) e;
                            logger.info("KEY_VALUE_PAIR: key= {}  valueType={}  value= {}", new String(kvp.getKey()), kvp.getValueType(), prettyString(kvp.getValues()));
                        } else if (e.getType() == EntryType.AUX){
                            Aux aux = (Aux) e;
                            logger.info("AUX: key= {}  value= {}",new String(aux.getKey()),new String(aux.getValue()));
                        } else if (e.getType() == EntryType.RESIZE_DB){
                            ResizeDb resizeDb = (ResizeDb) e;
                            logger.info("RESIZE_DB: getDbHashTableSize= {} getExpiryHashTableSize= {}", resizeDb.getDbHashTableSize(), resizeDb.getExpiryHashTableSize());
                        } else {
                            logger.info("unprocess type: {}", e.getType());
                        }
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    if (parser != null) {
                        try {
                            parser.close();
                        } catch (IOException e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }
                ch.config().setAutoRead(true);
                replyContext.updateRunidAndOffsetFromRDB();
                // 命令行模式
                replyContext.setStatus(ReplyStatus.ONLINE_MODE);
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

    private void sendCmdToHandler(List<Object> out, AbstractMsg<?> msg) {
        // logger.info("RedisCommandDecoder: Send command to next handler , cmd : "
        // + JsonUtils.toJSON(cmd));
        out.add(msg);
    }

    /**
     * 清楚内部变量状态值
     */
    private void clean() {
        this.redisCommand = null;
        this.argSize = 0;
        this.argIndex = 0;
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
            integer = (integer * 10) + (c - '0');
        }
        // skip \r
        // skip \n
        if (in.readByte() != '\n') {
            throw new IllegalStateException("Invalid number");
        }
        return integer;
    }

    private String readStringAndSkipCRCF(ByteBuf in) {
        StringBuffer sb = new StringBuffer();
        char c;
        while ((c = (char) in.readByte()) != '\r') {
            sb.append(c);
        }
        // skip \r
        // skip \n
        if (in.readByte() != '\n') {
            throw new IllegalStateException("Invalid number");
        } else {
        }

        return sb.toString();
    }

}
