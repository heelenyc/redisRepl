package redis.repl.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.repl.api.ReplyStatus;

/**
 * @author yicheng
 * @since 2016年10月20日
 * 
 */
public class ReplyContext {
    
    private Logger logger = LoggerFactory.getLogger(ReplyContext.class);

    private volatile String runID = "e491a9f300b6a2f08c825abceec014caf11a7961";
    private volatile long offset = 3925;  // 表示本地slave已经同步到哪里了，psync发送的时候带的offset是开始传输的位置，所以要用本地offset + 1
//    private volatile String runID = "?";
//    private volatile long offset = 0l;
    
    private String RDB_runID;
    private long RDB_offset;
    
    private volatile long lastiotimestmap = System.currentTimeMillis();

    private volatile ReplyStatus status = ReplyStatus.TO_SEND_PING;
    private volatile Boolean isAckScheduleStarted = false;

    public String getRunID() {
        return runID;
    }

    public void setRunID(String runID) {
        this.runID = runID;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
    
    public void incOffset(long inc) {
        if (status == ReplyStatus.ONLINE_MODE || status == ReplyStatus.FINISH_SEND_PSYNC) {
            this.offset = offset + inc;
        }
    }

    public ReplyStatus getStatus() {
        return status;
    }

    public void setStatus(ReplyStatus status) {
        this.status = status;
    }

    public long getLastiotimestmap() {
        return lastiotimestmap;
    }

    public void setLastiotimestmap(long lastiotimestmap) {
        this.lastiotimestmap = lastiotimestmap;
    }

    public Boolean getIsAckStarted() {
        return isAckScheduleStarted;
    }

    public void setIsAckStarted(Boolean isAckStarted) {
        this.isAckScheduleStarted = isAckStarted;
    }

    public String getRDB_runID() {
        return RDB_runID;
    }

    public void setRDB_runID(String rDB_runID) {
        RDB_runID = rDB_runID;
    }

    public long getRDB_offset() {
        return RDB_offset;
    }

    public void setRDB_offset(long rDB_offset) {
        RDB_offset = rDB_offset;
    }

    public void updateRunidAndOffsetFromRDB(){
        this.runID = this.RDB_runID;
        this.offset = this.RDB_offset;
        logger.info("after rdb, update runid = " + this.runID + " offset = " + this.offset);
    }
}
