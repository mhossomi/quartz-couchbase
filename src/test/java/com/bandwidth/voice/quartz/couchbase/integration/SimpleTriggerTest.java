package com.bandwidth.voice.quartz.couchbase.integration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.sql.Date;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.quartz.SchedulerException;

@Slf4j
public class SimpleTriggerTest extends IntegrationTestBase {

    @Test
    public void runsJobNow() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger().startNow());
        listener.await().get(1000, MILLISECONDS);
    }

    @Test
    public void runsFutureJob() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger()
                        .startAt(Date.from(Instant.now().plusSeconds(5))));
        listener.await().get(5000, MILLISECONDS);
    }

    @Test
    public void repeatsJob() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger()
                        .startNow()
                        .withSchedule(simpleSchedule()
                                .withRepeatCount(3)
                                .withIntervalInSeconds(1)));
        listener.await().get(1000, MILLISECONDS);
        listener.await().get(1000, MILLISECONDS);
        listener.await().get(1000, MILLISECONDS);
    }

    @Test
    public void reschedulesJob() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger().startNow());
        listener.await().thenAccept(context -> {
            try {
                log.info("Rescheduling: {}", context.getJobDetail().getKey());
                context.getScheduler().rescheduleJob(
                        context.getTrigger().getKey(),
                        newTrigger().startNow().build());
            }
            catch (SchedulerException e) {
                throw new RuntimeException(e);
            }
        }).get(1000, MILLISECONDS);
        listener.await().get(1000, MILLISECONDS);
    }
}
