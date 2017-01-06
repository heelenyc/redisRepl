package redis.repl.api;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * 应答接口
 * @author yicheng
 * @since 2016年1月11日
 *
 */
public abstract class AbstractRedisMsg<T> {

    public static final byte[] CRLF = new byte[] { '\r', '\n' };
    public static final String CRLFReadable = "/r/n";
    
    public abstract void write(ByteBuf out) throws IOException;
    
    public abstract T data();
    
    public abstract int getOffsetSize(); // 用来增长offset用
    
    public abstract String toRawString();
    
}
