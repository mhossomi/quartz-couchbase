package com.bandwidth.voice.quartz.couchbase;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

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
        Properties properties = new Properties();
        properties.put("org.quartz.jobStore.class", CouchbaseJobStore.class.getName());
        properties.put("org.quartz.threadPool.class", SimpleThreadPool.class.getName());
        properties.put("org.quartz.threadPool.threadCount", "10");

        SchedulerFactory factory = new StdSchedulerFactory(properties);
        scheduler = factory.getScheduler();
    }

    @After
    public void teardown() throws SchedulerException {
        if (scheduler.isStarted()) {
            scheduler.shutdown();
        }
    }

    @Test
    public void test() throws SchedulerException {
        scheduler.scheduleJob(
                newJob().withIdentity("job").ofType(TestJob.class).build(),
                newTrigger().withIdentity("trigger").startNow().build());
        Set<TriggerKey> keys = scheduler.getTriggerKeys(GroupMatcher.anyGroup());
        log.info("Keys: {}", keys);
    }

    @Slf4j
    public static class TestJob implements Job {
        public void execute(JobExecutionContext context) throws JobExecutionException {
            log.info("Run!");
        }
    }
}
