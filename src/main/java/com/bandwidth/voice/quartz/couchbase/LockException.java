package com.bandwidth.voice.quartz.couchbase;

import lombok.Getter;

@Getter
public class LockException extends RuntimeException {

    private final boolean retriable;

    public LockException(boolean retriable, String message) {
        super(message, null, false, false);
        this.retriable = retriable;
    }

    public LockException(boolean retriable, String message, Throwable cause) {
        super(message, cause, false, false);
        this.retriable = retriable;
    }
}
