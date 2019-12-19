package com.bandwidth.voice.quartz.couchbase.integration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.sql.Date;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class SimpleTriggerTest extends IntegrationTestBase {

    @Test
    public void runsJobNow() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger().startNow());
        listener.await().get(100, MILLISECONDS);
    }

    @Test
    public void runsJobAfter5s() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger().startAt(Date.from(Instant.now().plusSeconds(5))));
        listener.await().get(5100, MILLISECONDS);
    }

    @Test
    public void repeatsJob() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger()
                        .withSchedule(simpleSchedule()
                                .withRepeatCount(3)
                                .withIntervalInSeconds(1))
                        .startNow());
        listener.await().get(100, MILLISECONDS);
        listener.await().get(3000, MILLISECONDS);
        listener.await().get(3000, MILLISECONDS);
    }
}
