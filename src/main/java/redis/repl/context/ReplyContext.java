package redis.repl.context;

import redis.repl.api.ReplyStatus;

/**
 * @author yicheng
 * @since 2016年10月20日
 * 
 */
public class ReplyContext {

//    private volatile String runID = "1e3c43d89bbfaf8971c2d02c14a1be116d09f985";
//    private volatile long offset = 19546l - 5;
    private volatile String runID = "?";
    private volatile long offset = 0l;
    
    
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
        if (status == ReplyStatus.ONLINE_MODE) {
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

}
