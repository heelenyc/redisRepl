package redis.repl.api;

public enum ReplyStatus {
    TO_SEND_PING, FINISH_SEND_PING, TO_SEND_PSYNC, FINISH_SEND_PSYNC, TO_TRANSFER_RDB, ONLINE_MODE
}