package redis.repl.msg;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import redis.repl.api.AbstractRedisMsg;

/**
 * @author yicheng
 * @since 2016年10月19日
 * 
 */
public class ArrayMsg extends AbstractRedisMsg<List<AbstractRedisMsg<?>>> {

    private static final char MARKER = '*';

    private List<AbstractRedisMsg<?>> list;
    
    /**
     * 只包含字符串的数据构造
     */
    public ArrayMsg(List<String> bulks) {
        list = new ArrayList<AbstractRedisMsg<?>>();
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

            for (AbstractRedisMsg<?> element : list) {
                element.write(out);
            }
        } else {
            out.writeBytes("*-1\r\n".getBytes());
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("*" + list.size() + "\r\n");
        for ( AbstractRedisMsg<?> msg : list) {
            sb.append(msg.toString());
        }
        return sb.toString();
    }
    
    @Override
    public List<AbstractRedisMsg<?>> data() {
        return list;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
