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
    
    private int ByteSize = 0;
    
    public abstract void write(ByteBuf out) throws IOException;
    
    public abstract T data();
    
    public int getByteSize() {
        if (ByteSize == 0) {
            return toString().getBytes().length;
        }
        return ByteSize;
    }
    
    public void setByteSize(int byteSize) {
        ByteSize = byteSize;
    }
    
}
