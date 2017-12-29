package redis.repl.msg;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import redis.repl.api.AbstractMsg;

/**
 * @author yicheng
 * @since 2016年1月11日
 * 
 */
public class UnexpectedMsg extends AbstractMsg<Byte> {

    private final Byte data;

    public UnexpectedMsg(byte data) {
        this.data = data;
    }

    @Override
    public void write(ByteBuf out) throws IOException {
        // noop !
    }

    @Override
    public String toString() {
    	if ('\r' == data) {
    		return "/r";
		} else if ('\n' == data) {
    		return "/n";
		}
        return new String(new byte[]{data});
    }
    
    @Override
    public String toRawString() {
        return new String(new byte[]{data});
    }

    @Override
    public Byte data() {
        return data;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof UnexpectedMsg) {
            return data.equals(((UnexpectedMsg)obj).data());
        }
        return false;
    }

    @Override
    public int getOffsetSize() {
        return 1;
    }
}