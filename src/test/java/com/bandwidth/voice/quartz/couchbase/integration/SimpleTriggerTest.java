package com.bandwidth.voice.quartz.couchbase.integration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class SimpleTriggerTest extends IntegrationTestBase {

    @Test
    public void runsJobNow() throws Exception {
        ListenableJob.Listener listener = schedule(newJob(), newTrigger().startNow());
        listener.await()
                .thenAccept(context -> log.info("Completed: {}", context.getJobDetail().getKey()))
                .get(100, MILLISECONDS);
    }

}
