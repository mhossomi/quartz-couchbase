package com.bandwidth.voice.quartz.couchbase.integration;

import static java.time.Instant.now;
import static java.util.Date.from;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.concurrent.CompletableFuture;
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
                        .startAt(from(now().plusMillis(1500))));
        listener.await().get(1500, MILLISECONDS);
    }

    @Test
    public void repeatsJob() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger()
                        .startNow()
                        .withSchedule(simpleSchedule()
                                .withRepeatCount(2)
                                .withIntervalInMilliseconds(500)));
        listener.await().get(500, MILLISECONDS);
        listener.await().get(500, MILLISECONDS);
        listener.await().get(500, MILLISECONDS);
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

    @Test
    public void runsMultipleJobs() throws Exception {
        ListenableJob.Listener runNow = schedule(
                newJob(),
                newTrigger()
                        .startNow());
        ListenableJob.Listener runLater = schedule(
                newJob(),
                newTrigger()
                        .startAt(from(now().plusMillis(1500))));
        ListenableJob.Listener runMany = schedule(
                newJob(),
                newTrigger()
                        .startNow()
                        .withSchedule(simpleSchedule()
                                .withRepeatCount(2)
                                .withIntervalInMilliseconds(1000)));

        CompletableFuture.allOf(
                runNow.await(),
                runLater.await(),
                runMany.await(),
                runMany.await(),
                runMany.await())
                .get(5000, MILLISECONDS);
    }
}
