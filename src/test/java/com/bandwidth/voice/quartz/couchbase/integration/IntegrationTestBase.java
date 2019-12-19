package com.bandwidth.voice.quartz.couchbase.integration;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.bandwidth.voice.quartz.couchbase.CouchbaseJobStore;
import com.couchbase.client.java.CouchbaseCluster;
import java.util.Optional;
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

    private static Properties properties;

    static {
        try {
            properties = new Properties();
            properties.load(Optional.of(IntegrationTestBase.class.getClassLoader())
                    .map(loader -> loader.getResourceAsStream("config.properties"))
                    .orElseThrow(() -> new RuntimeException("Could not find config.properties")));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load properties", e);
        }
    }

    @Rule
    public TestName testName = new TestName();
    @Rule
    public Timeout timeout = new Timeout(10, SECONDS);

    protected Scheduler scheduler;
    protected String alias;
    protected AtomicInteger counter = new AtomicInteger(0);

    @BeforeClass
    public static void setupCouchbase() {
        CouchbaseJobStore.setCluster(CouchbaseCluster
                .create(properties.getProperty("couchbase.endpoint"))
                .authenticate(
                        properties.getProperty("couchbase.username"),
                        properties.getProperty("couchbase.password")));
    }

    @Before
    public void setup() throws SchedulerException {
        // Hopefully random enough
        alias = format("%08x", UUID.randomUUID().getLeastSignificantBits());
        log.info("Test {}.{} alias: {}", getClass().getSimpleName(), testName.getMethodName(), alias);

        Properties schedulerProperties = new Properties();
        schedulerProperties.put("org.quartz.scheduler.instanceName", getClass().getSimpleName() + "." + alias);
        schedulerProperties.put("org.quartz.scheduler.threadName", getClass().getSimpleName() + "." + alias);
        schedulerProperties.put("org.quartz.scheduler.idleWaitTime", "1000");
        schedulerProperties.put("org.quartz.jobStore.class", CouchbaseJobStore.class.getName());
        schedulerProperties.put("org.quartz.threadPool.class", SimpleThreadPool.class.getName());
        schedulerProperties.put("org.quartz.threadPool.threadCount", "3");
        schedulerProperties.put("org.quartz.jobStore.bucketName", properties.getProperty("couchbase.bucket"));

        SchedulerFactory factory = new StdSchedulerFactory(schedulerProperties);
        scheduler = factory.getScheduler();
        scheduler.start();
    }

    @After
    public void teardown() throws SchedulerException {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.shutdown(true);
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
