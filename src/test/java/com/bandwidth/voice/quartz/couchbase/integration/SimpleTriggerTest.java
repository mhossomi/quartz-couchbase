package com.bandwidth.voice.quartz.couchbase.integration;

import static java.time.Instant.now;
import static java.util.Date.from;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import com.bandwidth.voice.quartz.couchbase.integration.job.ListenableJob;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.quartz.SchedulerException;
import org.quartz.spi.JobStore;

@Slf4j
public class SimpleTriggerTest extends IntegrationTestBase {

    public SimpleTriggerTest(Class<? extends JobStore> jobStoreClass) {
        super(jobStoreClass);
    }

    @Test
    public void runsJobNow() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger().startNow());
        listener.expectExecution().get(1000, MILLISECONDS);
    }

    @Test
    public void runsFutureJob() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger()
                        .startAt(from(now().plusMillis(1500))));
        listener.expectExecution().get(3000, MILLISECONDS);
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
        listener.expectExecution().get(1000, MILLISECONDS);
        listener.expectExecution().get(1000, MILLISECONDS);
        listener.expectExecution().get(1000, MILLISECONDS);
    }

    @Test
    public void reschedulesJob() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger().startNow());
        listener.expectExecution().thenAccept(context -> {
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
        listener.expectExecution().get(1000, MILLISECONDS);
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
                runNow.expectExecution(),
                runLater.expectExecution(),
                runMany.expectExecution(),
                runMany.expectExecution(),
                runMany.expectExecution())
                .get(3000, MILLISECONDS);
    }

    @Test
    public void runsJobWithMultipleTriggers() throws Exception {
        ListenableJob.Listener listener = schedule(
                newJob(),
                newTrigger().startNow(),
                newTrigger().startAt(from(now().plusMillis(1500))));
        listener.expectExecution().get(2000, MILLISECONDS);
        listener.expectExecution().get(2000, MILLISECONDS);
    }
}
