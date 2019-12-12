package com.bandwidth.voice.quartz.couchbase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;

import com.couchbase.client.java.CouchbaseCluster;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;

@Slf4j
public class CouchbaseJobStoreIntegrationTest {

    private static Map<JobKey, CompletableFuture<JobExecutionContext>> futures = new ConcurrentHashMap<>();

    private ExecutorService executor = Executors.newFixedThreadPool(10);
    private Scheduler scheduler;

    @Before
    public void setup() throws SchedulerException {
        CouchbaseJobStore.setCluster(CouchbaseCluster
                .create("docker:8091")
                .authenticate("Administrator", "password"));

        Properties properties = new Properties();
        properties.put("org.quartz.jobStore.class", CouchbaseJobStore.class.getName());
        properties.put("org.quartz.threadPool.class", SimpleThreadPool.class.getName());
        properties.put("org.quartz.threadPool.threadCount", "10");
        properties.put("org.quartz.jobStore.bucketName", "quartz");

        SchedulerFactory factory = new StdSchedulerFactory(properties);
        scheduler = factory.getScheduler();
        scheduler.start();
    }

    @After
    public void teardown() throws SchedulerException {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.shutdown();
        }
    }

    @Test(timeout = 3000)
    public void runsJobNow() throws Exception {
        JobDetail job = newListenableJob().build();
        var future = scheduleAndListen(job, newTrigger().startNow().build());

        JobExecutionContext context = future.join();
        assertThat(context.getJobDetail().getKey())
                .isEqualTo(job.getKey());
    }

    private JobBuilder newListenableJob() {
        JobKey jobKey = jobKey(UUID.randomUUID().toString());
        return newJob(ListenableJob.class)
                .withIdentity(jobKey);
    }

    private CompletableFuture<JobExecutionContext> scheduleAndListen(JobDetail job, Trigger trigger) {
        CompletableFuture<JobExecutionContext> future = new CompletableFuture<>();
        futures.put(job.getKey(), future);
        executor.execute(() -> {
            try {
                scheduler.scheduleJob(job, trigger);
            }
            catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Slf4j
    @AllArgsConstructor
    public static class ListenableJob implements Job {
        public void execute(JobExecutionContext context) {
            JobKey key = context.getJobDetail().getKey();
            log.info("Completed: {}", key);
            futures.get(key).complete(context);
        }
    }
}
