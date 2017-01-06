package redis.repl.msg;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import redis.repl.api.AbstractRedisMsg;

/**
 * @author yicheng
 * @since 2016年1月11日
 * 
 */
public class ErrorMsg extends AbstractRedisMsg<String> {

    private static final char MARKER = '-';

    private final String data;

    public ErrorMsg(byte[] data) {
        this.data = new String(data);
    }
    
    public ErrorMsg(String data) {
        if (data == null) {
            this.data = "";
        }else {
            this.data = data;
        }
    }


    @Override
    public void write(ByteBuf out) throws IOException {
        out.writeByte(MARKER);
        out.writeBytes(data.getBytes());
        out.writeBytes(CRLF);
    }

    @Override
    public String toString() {
        return "-" + data + CRLFReadable;
    }
    
    @Override
    public String toRawString() {
        return "-" + data + "\r\n";
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ErrorMsg) {
            return data.equals(((ErrorMsg)obj).data());
        }
        return false;
    }

    @Override
    public String data() {
        return data;
    }

    @Override
    public int getOffsetSize() {
        return data.length() + 3;
    }

}
