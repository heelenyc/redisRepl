package redis.repl.msg.redis;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import redis.repl.api.AbstractMsg;

/**
 * @author yicheng
 * @since 2016年1月11日
 * 
 */
public class IntegerMsg extends AbstractMsg<Integer> {

    public static final IntegerMsg OK = new IntegerMsg(1);
    public static final IntegerMsg ERROR = new IntegerMsg(0);

    private static final char MARKER = ':';

    private final Integer data;

    public IntegerMsg(int data) {
        this.data = data;
    }

    @Override
    public void write(ByteBuf out) throws IOException {
        out.writeByte(MARKER);
        out.writeBytes(String.valueOf(data).getBytes());
        out.writeBytes(CRLF);
    }

    @Override
    public String toString() {
        return ":" + data + CRLFReadable;
    }
    
    @Override
    public String toRawString() {
        return ":" + data + "\r\n";
    }

    @Override
    public Integer data() {
        return data;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntegerMsg) {
            return data.equals(((IntegerMsg)obj).data());
        }
        return false;
    }

    @Override
    public int getOffsetSize() {
        return String.valueOf(data).length() + 3;
    }
}