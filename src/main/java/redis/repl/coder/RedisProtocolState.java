package redis.repl.coder;

/**
 * @author yicheng
 * @since 2016年2月4日
 *
 */
public enum RedisProtocolState {

    TO_READ_PREFIX,
    TO_READ_SIMPLESTR,
    TO_READ_ERROR,
    TO_READ_INTEGER,
    TO_READ_BULK,
    TO_READ_ARRAY_NUM,
    TO_READ_ARRAY_ARGS,
    
    TO_READ_RDB_LEN,
    TO_READ_RDB_BODY,
}
