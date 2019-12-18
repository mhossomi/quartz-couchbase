package com.bandwidth.voice.quartz.couchbase;

import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.sleepQuietly;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.ACQUIRED;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.COMPLETE;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.ERROR;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.READY;

import com.bandwidth.voice.quartz.couchbase.CouchbaseDelegate.AcquiredLock;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;

@Slf4j
@NoArgsConstructor
public class CouchbaseJobStore implements JobStore {

    private static final int MAX_LOCK_TRIES = 3;
    private static final int MAX_ACQUIRE_TRIES = 3;
    private static final String LOCK_TRIGGER_ACCESS = "trigger-access";

    @Setter
    private static Cluster cluster;
    @Setter
    private String bucketName;
    @Setter
    private String bucketPassword;
    @Setter
    private String instanceName;
    @Setter
    private String instanceId;
    @Setter
    private int threadPoolSize;

    private CouchbaseDelegate couchbase;

    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        log.info("Initializing {} with bucket '{}'", getClass().getSimpleName(), bucketName);
        Bucket bucket = Optional.ofNullable(bucketPassword)
                .map(password -> cluster.openBucket(bucketName, password))
                .orElseGet(() -> cluster.openBucket(bucketName));
        couchbase = new CouchbaseDelegate(instanceName, bucket);
    }

    public void schedulerStarted() throws SchedulerException {

    }

    public void schedulerPaused() {

    }

    public void schedulerResumed() {

    }

    public void shutdown() {
        if (couchbase != null && !couchbase.isShutdown()) {
            couchbase.shutdown();
        }
    }

    public boolean supportsPersistence() {
        return true;
    }

    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        return 0;
    }

    public boolean isClustered() {
        return true;
    }

    public void storeJobAndTrigger(JobDetail job, OperableTrigger trigger) throws JobPersistenceException {
        log.debug("storeJobAndTrigger: {}, {}", job, trigger);
        executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            couchbase.storeJob(job);
            couchbase.storeTrigger(trigger);
            return null;
        }, () -> {
            couchbase.removeJob(job.getKey());
            couchbase.removeTrigger(trigger.getKey());
        });
    }

    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws JobPersistenceException {
        log.debug("storeJobsAndTriggers: {}, {}", triggersAndJobs, replace);
        executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            for (var triggerAndJob : triggersAndJobs.entrySet()) {
                couchbase.storeJob(triggerAndJob.getKey());
                for (Trigger trigger : triggerAndJob.getValue()) {
                    couchbase.storeTrigger((OperableTrigger) trigger);
                }
            }
            return null;
        }, () -> {
            for (var triggerAndJob : triggersAndJobs.entrySet()) {
                if (couchbase.removeJob(triggerAndJob.getKey().getKey())) {
                    for (Trigger trigger : triggerAndJob.getValue()) {
                        couchbase.removeTrigger(trigger.getKey());
                    }
                }
            }
        });
    }

    public void storeJob(JobDetail job, boolean replaceExisting) throws JobPersistenceException {
        log.debug("storeJob: {}, {}", job, replaceExisting);
        executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            couchbase.storeJob(job);
            return null;
        });
    }

    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        log.debug("removeJob: {}", jobKey);
        return executeInLock(LOCK_TRIGGER_ACCESS, () -> couchbase.removeJob(jobKey));
    }

    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        log.debug("removeJobs: {}", jobKeys);
        return executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            boolean foundAll = true;
            for (JobKey jobKey : jobKeys) {
                foundAll = couchbase.removeJob(jobKey) && foundAll;
            }
            return foundAll;
        });
    }

    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        log.debug("retrieveJob: {}", jobKey);
        return couchbase.retrieveJob(jobKey).orElse(null);
    }

    public void storeTrigger(OperableTrigger trigger, boolean replaceExisting) throws JobPersistenceException {
        log.debug("storeTrigger: {}, {}", trigger, replaceExisting);
        executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            couchbase.storeTrigger(trigger);
            return null;
        });
    }

    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("removeTrigger: {}", triggerKey);
        executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            couchbase.removeTrigger(triggerKey);
            return null;
        });
        return false;
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        log.debug("removeTriggers: {}", triggerKeys);
        return executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            boolean foundAll = true;
            for (TriggerKey triggerKey : triggerKeys) {
                foundAll = couchbase.removeTrigger(triggerKey) && foundAll;
            }
            return foundAll;
        });
    }

    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        log.debug("replaceTrigger: {}, {}", triggerKey, newTrigger);
        return false;
    }

    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("retrieveTrigger: {}", triggerKey);
        return couchbase.retrieveTrigger(triggerKey).orElse(null);
    }

    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        log.debug("checkExists: {}", jobKey);
        return couchbase.retrieveJob(jobKey).isPresent();
    }

    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("checkExists: {}", triggerKey);
        return couchbase.retrieveTrigger(triggerKey).isPresent();
    }

    public void clearAllSchedulingData() throws JobPersistenceException {
        log.debug("clearAllSchedulingData");
    }

    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
            throws ObjectAlreadyExistsException, JobPersistenceException {

    }

    public boolean removeCalendar(String calName) throws JobPersistenceException {
        return false;
    }

    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        return null;
    }

    public int getNumberOfJobs() throws JobPersistenceException {
        return 0;
    }

    public int getNumberOfTriggers() throws JobPersistenceException {
        return 0;
    }

    public int getNumberOfCalendars() throws JobPersistenceException {
        return 0;
    }

    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        log.debug("getJobKeys: {}", matcher);
        return null;
    }

    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        log.debug("getTriggerKeys: {}", matcher);
        return null;
    }

    public List<String> getJobGroupNames() throws JobPersistenceException {
        return null;
    }

    public List<String> getTriggerGroupNames() throws JobPersistenceException {
        return null;
    }

    public List<String> getCalendarNames() throws JobPersistenceException {
        return null;
    }

    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        log.debug("getTriggersForJob: {}", jobKey);
        return null;
    }

    public Trigger.TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("getTriggerState: {}", triggerKey);
        return null;
    }

    public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("resetTriggerFromErrorState: {}", triggerKey);
    }

    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("pauseTrigger: {}", triggerKey);
    }

    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        log.debug("pauseTriggers: {}", matcher);
        return null;
    }

    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        log.debug("pauseJob: {}", jobKey);
    }

    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        log.debug("pauseJobs: {}", groupMatcher);
        return null;
    }

    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("resumeTrigger: {}", triggerKey);
    }

    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        log.debug("resumeTriggers: {}", matcher);
        return null;
    }

    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        log.debug("getPausedTriggerGroups");
        return null;
    }

    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        log.debug("resumeJob: {}", jobKey);
    }

    public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        log.debug("resumeJobs: {}", matcher);
        return null;
    }

    public void pauseAll() throws JobPersistenceException {
        log.debug("pauseAll");
    }

    public void resumeAll() throws JobPersistenceException {
        log.debug("resumeAll");
    }

    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
            throws JobPersistenceException {
        log.debug("acquireNextTriggers: {}, {}, {}", noLaterThan, maxCount, timeWindow);

        List<OperableTrigger> triggers = new ArrayList<>();
        for (int tries = 0; tries < MAX_ACQUIRE_TRIES && triggers.isEmpty(); tries++) {
            log.debug("Attempt {}/{} to acquire next triggers", tries, MAX_ACQUIRE_TRIES);
            long maxNextFireTime = Math.max(noLaterThan, System.currentTimeMillis()) + timeWindow;

            List<TriggerKey> triggerKeys = couchbase.selectTriggerKeys(
                    instanceName, READY, maxCount, maxNextFireTime);
            log.debug("Trying to acquire {} triggers", triggerKeys.size());

            for (TriggerKey triggerKey : triggerKeys) {
                log.debug("Trying to acquire trigger: {}", triggerKey);
                couchbase.updateTriggerState(triggerKey, READY, ACQUIRED).ifPresent(trigger -> {
                    log.debug("Acquired trigger: {}", trigger.getKey());
                    triggers.add(trigger);
                });
            }
        }
        return triggers;
    }

    public void releaseAcquiredTrigger(OperableTrigger trigger) {
        log.debug("releaseAcquiredTrigger: {}", trigger);
    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        log.debug("triggersFired: {}", triggers);
        List<TriggerFiredResult> result = new ArrayList<>();
        for (OperableTrigger trigger : triggers) {
            Optional<JobDetail> triggerJob = couchbase.retrieveJob(trigger.getJobKey());
            if (triggerJob.isPresent()) {
                JobDetail job = triggerJob.get();
                Date scheduledFireTime = trigger.getPreviousFireTime();
                trigger.triggered(null);
                log.debug("Fired trigger {} with job {}", trigger.getKey(), job.getKey());
                result.add(new TriggerFiredResult(new TriggerFiredBundle(
                        job, trigger, null, false,
                        new Date(), scheduledFireTime, trigger.getPreviousFireTime(), trigger.getNextFireTime())));
            }
        }
        return result;
    }

    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
            CompletedExecutionInstruction instruction) {
        log.debug("triggeredJobComplete: {}, {}, {}", trigger, jobDetail, instruction);
        retryWhileAvailable(() -> executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            TriggerKey triggerKey = trigger.getKey();
            switch (instruction) {
                case SET_TRIGGER_COMPLETE:
                    couchbase.updateTriggerState(triggerKey, COMPLETE);
                    break;
                case SET_TRIGGER_ERROR:
                    couchbase.updateTriggerState(triggerKey, ERROR);
                    break;
                case DELETE_TRIGGER:
                    couchbase.removeTrigger(triggerKey);
                    break;
                case SET_ALL_JOB_TRIGGERS_COMPLETE:
                case SET_ALL_JOB_TRIGGERS_ERROR:
                    log.warn("Not supported: {}", instruction);
                    break;
            }
            return null;
        }));
    }

    public long getAcquireRetryDelay(int failureCount) {
        return 0;
    }

    private <T> T executeInLock(String lockName, PersistenceSupplier<T> action) throws JobPersistenceException {
        return executeInLock(lockName, action, () -> { });
    }

    private <T> T executeInLock(String lockName, PersistenceSupplier<T> action, PersistenceRunnable rollback)
            throws JobPersistenceException {
        int tries = 0;
        do {
            tries++;
            try (AcquiredLock lock = couchbase.getLock(lockName)) {
                return action.get();
            }
            catch (LockException e) {
                if (e.isRetriable()) {
                    log.warn("Failed to acquire lock {} (attempt {}/{})", lockName, tries, MAX_LOCK_TRIES, e);
                    sleepQuietly(100);
                }
                else {
                    log.error("Failed to acquire lock {}", lockName, e);
                    throw e;
                }
            }
            catch (Exception e) {
                log.warn("Execution failed!", e);
                rollback.run();
                throw e;
            }
        }
        while (tries < MAX_LOCK_TRIES);

        rollback.run();
        throw new JobPersistenceException(String.format("Failed to acquire lock %s after %d attempts.",
                lockName, MAX_LOCK_TRIES));
    }

    private <T> T retryWhileAvailable(PersistenceSupplier<T> action) {
        while (!couchbase.isShutdown()) {
            try {
                return action.get();
            }
            catch (Exception e) {
                log.debug("Failed to execute, but insisting on it.", e);
            }
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                throw new IllegalStateException("Interrupted while waiting.", e);
            }
        }
        throw new IllegalStateException("No longer available");
    }

    private interface PersistenceSupplier<T> {
        T get() throws JobPersistenceException;
    }

    private interface PersistenceRunnable {
        void run() throws JobPersistenceException;
    }

}
