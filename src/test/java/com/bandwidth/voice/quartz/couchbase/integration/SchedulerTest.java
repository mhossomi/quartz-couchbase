package com.bandwidth.voice.quartz.couchbase.integration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import com.bandwidth.voice.quartz.couchbase.util.TestUtils;
import com.bandwidth.voice.quartz.couchbase.util.TestUtils.CountResult;
import com.bandwidth.voice.quartz.couchbase.integration.job.TestJob;
import com.couchbase.client.deps.io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.quartz.spi.JobStore;

public class SchedulerTest extends IntegrationTestBase {

    private ExecutorService executor;

    public SchedulerTest(Class<? extends JobStore> jobStoreClass) {
        super(jobStoreClass);
    }

    @Before
    public void setupExecutor() {
        executor = Executors.newFixedThreadPool(10, new DefaultThreadFactory(getClass().getSimpleName() + "." + alias));
    }

    @Test
    public void handlesConcurrentJobScheduling() throws InterruptedException, ExecutionException, TimeoutException {
        CountResult result = TestUtils.count(5, executor, i -> () -> scheduler.scheduleJob(
                newJob(TestJob.class)
                        .withIdentity(jobKey(0))
                        .storeDurably()
                        .build(),
                newTrigger()
                        .withIdentity(triggerKey(i))
                        .startNow()
                        .build()))
                .get(5000, MILLISECONDS);

        log.info("Result: {}", result);
        assertThat(result.getPassCount()).isEqualTo(1);
        assertThat(result.getFailCount()).isEqualTo(4);
    }
}
