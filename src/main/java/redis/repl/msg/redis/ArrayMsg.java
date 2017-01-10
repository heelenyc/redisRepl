package redis.repl.msg.redis;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import redis.repl.api.AbstractMsg;

/**
 * @author yicheng
 * @since 2016年10月19日
 * 
 */
public class ArrayMsg extends AbstractMsg<List<AbstractMsg<?>>> {

    private static final char MARKER = '*';

    private List<AbstractMsg<?>> list;
    
    /**
     * 只包含字符串的数据构造
     */
    public ArrayMsg(List<String> bulks) {
        list = new ArrayList<AbstractMsg<?>>();
        for (String bulk : bulks) {
            list.add(new BulkMsg(bulk));
        }
    }
    @Override
    public void write(ByteBuf out) throws IOException {
        if (list.size() >= 0) {
            out.writeByte(MARKER);
            out.writeBytes(String.valueOf(list.size()).getBytes());
            out.writeBytes(CRLF);

            for (AbstractMsg<?> element : list) {
                element.write(out);
            }
        } else {
            out.writeBytes("*-1\r\n".getBytes());
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("*" + list.size() + CRLFReadable);
        for ( AbstractMsg<?> msg : list) {
            sb.append(msg.toString());
        }
        return sb.toString();
    }
    
    public String toRawString() {
        StringBuffer sb = new StringBuffer();
        sb.append("*" + list.size() + "\r\n");
        for ( AbstractMsg<?> msg : list) {
            sb.append(msg.toRawString());
        }
        return sb.toString();
    }
    
    
    
    @Override
    public List<AbstractMsg<?>> data() {
        return list;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
    @Override
    public int getOffsetSize() {
        Integer size = 1;
        size += String.valueOf(list.size()).length() + 2;
        for ( AbstractMsg<?> msg : list) {
            size += msg.getOffsetSize();
        }
        return size;
    }
}
