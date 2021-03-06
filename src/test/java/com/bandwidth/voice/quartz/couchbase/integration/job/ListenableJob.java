package com.bandwidth.voice.quartz.couchbase.integration.job;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;

@Slf4j
public class ListenableJob implements Job {

    private static Map<JobKey, Listener> listeners = new ConcurrentHashMap<>();

    public static Listener schedule(Scheduler scheduler, JobBuilder jobBuilder, TriggerBuilder<?> triggerBuilder)
            throws SchedulerException {
        JobDetail job = jobBuilder.ofType(ListenableJob.class).build();
        scheduler.scheduleJob(job, triggerBuilder.build());
        return listeners.computeIfAbsent(job.getKey(), Listener::new);
    }

    public static Listener schedule(Scheduler scheduler, JobBuilder jobBuilder, Set<TriggerBuilder<?>> triggerBuilders)
            throws SchedulerException {
        JobDetail job = jobBuilder.ofType(ListenableJob.class).build();
        scheduler.scheduleJob(job, triggerBuilders.stream().map(TriggerBuilder::build).collect(toSet()), false);
        return listeners.computeIfAbsent(job.getKey(), Listener::new);
    }

    @Override
    public void execute(JobExecutionContext context) {
        JobKey key = context.getJobDetail().getKey();
        log.info("Execute: {}", key);

        try {
            Listener listener = listeners.computeIfAbsent(key, Listener::new);
            var future = listener.futures.poll(1, TimeUnit.SECONDS);

            if (future != null) {
                future.complete(context);
            }
            else {
                log.debug("No callback for job {}", key);
            }
        }
        catch (InterruptedException e) {
            log.info("Job {} interrupted", key);
        }
    }

    @AllArgsConstructor
    public static class Listener {

        private final JobKey key;
        private final BlockingQueue<CompletableFuture<JobExecutionContext>> futures = new LinkedBlockingQueue<>();

        public CompletableFuture<JobExecutionContext> expectExecution() {
            var future = new CompletableFuture<JobExecutionContext>();
            futures.add(future);
            return future.thenApply(context -> {
                assertThat(context.getJobDetail().getKey()).isEqualTo(key);
                return context;
            });
        }
    }
}
