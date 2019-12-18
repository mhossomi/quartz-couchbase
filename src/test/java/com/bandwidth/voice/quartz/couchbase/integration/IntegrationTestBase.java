package com.bandwidth.voice.quartz.couchbase.integration;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.bandwidth.voice.quartz.couchbase.CouchbaseJobStore;
import com.couchbase.client.java.CouchbaseCluster;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.quartz.JobBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;

@Slf4j
public class IntegrationTestBase {

    @Rule
    public TestName testName = new TestName();
    @Rule
    public Timeout timeout = new Timeout(3, SECONDS);

    protected Scheduler scheduler;
    protected String alias;
    protected AtomicInteger counter = new AtomicInteger(0);

    @BeforeClass
    public static void setupCouchbase() {
        CouchbaseJobStore.setCluster(CouchbaseCluster
                .create("docker:8091")
                .authenticate("Administrator", "password"));
    }

    @Before
    public void setup() throws SchedulerException {
        // Hopefully random enough
        alias = String.format("%08x", UUID.randomUUID().getLeastSignificantBits());
        log.info("Test {}.{} alias: {}", getClass().getSimpleName(), testName.getMethodName(), alias);

        Properties properties = new Properties();
        properties.put("org.quartz.scheduler.instanceName", getClass().getSimpleName() + "." + alias);
        properties.put("org.quartz.scheduler.threadName", getClass().getSimpleName() + "." + alias);
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

    public ListenableJob.Listener schedule(JobBuilder job, TriggerBuilder<?> trigger) throws SchedulerException {
        int i = counter.getAndIncrement();
        return ListenableJob.schedule(scheduler,
                job.withIdentity(alias + "-" + i, getClass().getSimpleName()),
                trigger.withIdentity(alias + "-" + i, getClass().getSimpleName()));
    }

    public ListenableJob.Listener schedule(TriggerBuilder<?> trigger) throws SchedulerException {
        int i = counter.getAndIncrement();
        return ListenableJob.schedule(scheduler,
                trigger.withIdentity(alias + "-" + i, getClass().getSimpleName()));
    }
}
