package com.bandwidth.voice.quartz.couchbase;

import java.util.Date;
import org.quartz.JobDetail;
import org.quartz.Trigger;

public class CouchbaseUtils {

    public static String jobId(JobDetail job) {
        return "J." + job.getKey().toString();
    }

    public static String triggerId(Trigger trigger) {
        return "T." + trigger.getKey().toString();
    }

    static String lockId(String schedulerName, String lockName) {
        return String.format("L.%s.%s", schedulerName, lockName);
    }

    public static String serialize(Date date) {
        return date != null
                ? date.toInstant().toString()
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
