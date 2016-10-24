package redis.repl.cmd;

import java.util.Arrays;

import redis.repl.api.AbstractRedisMsg;
import redis.repl.msg.ArrayMsg;
import redis.repl.msg.SimpleStringMsg;

/**
 * @author yicheng
 * @since 2016年10月20日
 *
 */
public class CommonCmdUtil {

    public final static ArrayMsg PING = new ArrayMsg(Arrays.asList("PING"));
    
    public final static SimpleStringMsg PONG = new SimpleStringMsg("PONG");
    
    
    public static ArrayMsg newPyncCmd(String runid,long offset){
        return new ArrayMsg(Arrays.asList("PSYNC",runid,String.valueOf(offset)));
    }
    
    public static ArrayMsg newReplACKCmd(long offset){
        return new ArrayMsg(Arrays.asList("REPLCONF","ACK",String.valueOf(offset)));
    }
    
    public static boolean isFullSyncmd(AbstractRedisMsg<?> msg){
        if (msg instanceof SimpleStringMsg) {
            SimpleStringMsg cmd = (SimpleStringMsg)msg;
            return cmd.data().startsWith("FULLRESYNC");
        }
        return false;
    }
    
    public static boolean isContinueCmd(AbstractRedisMsg<?> msg){
        if (msg instanceof SimpleStringMsg) {
            SimpleStringMsg cmd = (SimpleStringMsg)msg;
            return cmd.data().startsWith("CONTINUE");
        }
        return false;
    }
}
