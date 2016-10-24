package redis.repl.msg;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import redis.repl.api.AbstractRedisMsg;

/**
 * @author yicheng
 * @since 2016年1月11日
 * 
 */
public class BulkMsg extends AbstractRedisMsg<String> {

    public static final BulkMsg NIL_REPLY = new BulkMsg();

    private static final char MARKER = '$';

    private final String data;
    private final int len;

    public BulkMsg() {
        this.data = null;
        this.len = -1;
    }

    public BulkMsg(String data) {
        this.data = data;
        this.len = data.length();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BulkMsg) {
            return data.equals(((BulkMsg)obj).data());
        }
        return false;
    }

    @Override
    public String data() {
        return data;
    }
    
    @Override
    public void write(ByteBuf out) throws IOException {
        // 1.Write header
        out.writeByte(MARKER);
        out.writeBytes(String.valueOf(len).getBytes());
        out.writeBytes(CRLF);

        // 2.Write data
        if (len > 0) {
            out.writeBytes(data.getBytes());
            out.writeBytes(CRLF);
        }
    }

    @Override
    public String toString() {
        return "$" + String.valueOf(len) + "\r\n"+ data + "\r\n";
    }
    
    
}
