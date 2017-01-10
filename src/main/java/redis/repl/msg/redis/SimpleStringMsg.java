package redis.repl.msg.redis;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import redis.repl.api.AbstractMsg;

/**
 * @author yicheng
 * @since 2016年1月11日
 * 
 */
public class SimpleStringMsg extends AbstractMsg<String> {

    public static final SimpleStringMsg OK = new SimpleStringMsg(new byte[] { 'o', 'k' });

    private static final char MARKER = '+';

    private final String data;

    public SimpleStringMsg(byte[] bytedata) {
        this.data = new String(bytedata);
    }
    
    public SimpleStringMsg(String data) {
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
        return "+" + data + CRLFReadable;
    }
    
    @Override
    public String toRawString() {
        return "+" + data + "\r\n";
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SimpleStringMsg) {
            return data.equals(((SimpleStringMsg)obj).data());
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
