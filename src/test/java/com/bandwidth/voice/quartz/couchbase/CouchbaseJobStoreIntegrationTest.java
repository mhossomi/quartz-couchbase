package com.bandwidth.voice.quartz.couchbase;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import com.couchbase.client.java.CouchbaseCluster;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.simpl.SimpleThreadPool;

@Slf4j
public class CouchbaseJobStoreIntegrationTest {

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
    }

    @After
    public void teardown() throws SchedulerException {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.shutdown();
        }
    }

    @Test
    public void test() throws SchedulerException, InterruptedException {
        Thread t1 = new Thread(() -> {
            try {
                scheduler.scheduleJob(
                        newJob().withIdentity("job-1").ofType(TestJob.class).build(),
                        newTrigger().withIdentity("trigger-1").startNow().build());
            }
            catch (SchedulerException e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                scheduler.scheduleJob(
                        newJob().withIdentity("job-2").ofType(TestJob.class).build(),
                        newTrigger().withIdentity("trigger-2").startNow().build());
            }
            catch (SchedulerException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    @Slf4j
    public static class TestJob implements Job {
        public void execute(JobExecutionContext context) throws JobExecutionException {
            log.info("Run!");
        }
    }
}
