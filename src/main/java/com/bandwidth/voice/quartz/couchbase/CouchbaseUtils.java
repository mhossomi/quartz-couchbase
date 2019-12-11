package com.bandwidth.voice.quartz.couchbase;

import java.time.Instant;
import java.util.Date;
import org.quartz.JobKey;
import org.quartz.TriggerKey;

public class CouchbaseUtils {

    public static String jobId(JobKey key) {
        return "J." + key.toString();
    }

    public static String triggerId(TriggerKey key) {
        return "T." + key.toString();
    }

    static String lockId(String schedulerName, String lockName) {
        return String.format("L.%s.%s", schedulerName, lockName);
    }

    public static String serialize(Date date) {
        return date != null
                ? date.toInstant().toString()
                : null;
    }

    public static Date parse(String date) {
        return date != null
                ? Date.from(Instant.parse(date))
                : null;
    }

    public static void sleepQuietly(int millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
        }
    }
}
