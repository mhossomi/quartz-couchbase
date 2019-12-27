package com.bandwidth.voice.quartz.couchbase.util;

@FunctionalInterface
public interface ThrowingRunnable {
    void run() throws Exception;
}
