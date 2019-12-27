package com.bandwidth.voice.quartz.couchbase;

import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.sleepQuietly;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.ACQUIRED;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.COMPLETE;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.ERROR;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.READY;

import com.bandwidth.voice.quartz.couchbase.CouchbaseDelegate.AcquiredLock;
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
    private int maxLockTime = 5;
    @Setter
    private int maxLockRetries = 10;
    @Setter
    private int maxAcquireRetries = 3;
    @Setter
    private int lockRetryDelay = 100;
    @Setter
    private int threadPoolSize;

    private CouchbaseDelegate couchbase;

    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        log.info("Initializing {} with bucket '{}'", getClass().getSimpleName(), bucketName);
        couchbase = CouchbaseDelegate.builder()
                .schedulerName(instanceName)
                .bucket(Optional.ofNullable(bucketPassword)
                        .map(password -> cluster.openBucket(bucketName, password))
                        .orElseGet(() -> cluster.openBucket(bucketName)))
                .maxLockTime(maxLockTime)
                .build();
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
        log.trace("storeJobAndTrigger: {}, {}", job, trigger);
        executeInLock("storeJobAndTrigger", LOCK_TRIGGER_ACCESS, () -> {
            couchbase.storeJob(job, false);
            couchbase.storeTrigger(trigger, READY, false);
            return null;
        });
    }

    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws JobPersistenceException {
        log.trace("storeJobsAndTriggers: {}, {}", triggersAndJobs, replace);
        executeInLock("storeJobAndTriggers", LOCK_TRIGGER_ACCESS, () -> {
            for (var triggerAndJob : triggersAndJobs.entrySet()) {
                couchbase.storeJob(triggerAndJob.getKey(), replace);
                for (Trigger trigger : triggerAndJob.getValue()) {
                    couchbase.storeTrigger((OperableTrigger) trigger, READY, replace);
                }
            }
            return null;
        });
    }

    public void storeJob(JobDetail job, boolean replaceExisting) throws JobPersistenceException {
        log.trace("storeJob: {}, {}", job, replaceExisting);
        executeInLock("storeJob", LOCK_TRIGGER_ACCESS, () -> {
            couchbase.storeJob(job, replaceExisting);
            return null;
        });
    }

    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        log.trace("removeJob: {}", jobKey);
        return executeInLock("removeJob", LOCK_TRIGGER_ACCESS, () -> couchbase.removeJob(jobKey));
    }

    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        log.trace("removeJobs: {}", jobKeys);
        return executeInLock("removeJobs", LOCK_TRIGGER_ACCESS, () -> {
            boolean foundAll = true;
            for (JobKey jobKey : jobKeys) {
                foundAll = couchbase.removeJob(jobKey) && foundAll;
            }
            return foundAll;
        });
    }

    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        log.trace("retrieveJob: {}", jobKey);
        return couchbase.retrieveJob(jobKey).orElse(null);
    }

    public void storeTrigger(OperableTrigger trigger, boolean replaceExisting) throws JobPersistenceException {
        log.trace("storeTrigger: {}, {}", trigger, replaceExisting);
        executeInLock("storeTrigger", LOCK_TRIGGER_ACCESS, () -> {
            couchbase.storeTrigger(trigger, READY, replaceExisting);
            return null;
        });
    }

    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.trace("removeTrigger: {}", triggerKey);
        executeInLock("removeTrigger", LOCK_TRIGGER_ACCESS, () -> {
            couchbase.removeTrigger(triggerKey);
            return null;
        });
        return false;
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        log.trace("removeTriggers: {}", triggerKeys);
        return executeInLock("removeTriggers", LOCK_TRIGGER_ACCESS, () -> {
            boolean foundAll = true;
            for (TriggerKey triggerKey : triggerKeys) {
                foundAll = couchbase.removeTrigger(triggerKey) && foundAll;
            }
            return foundAll;
        });
    }

    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        return executeInLock("replaceTrigger", LOCK_TRIGGER_ACCESS, () -> {
            Optional<JobDetail> triggerJob = couchbase.retrieveTriggerJob(triggerKey);
            if (triggerJob.isEmpty()) {
                return false;
            }

            boolean removed = couchbase.removeTrigger(triggerKey);
            couchbase.storeJob(triggerJob.get(), false);
            couchbase.storeTrigger(newTrigger, READY, false);
            return removed;
        });
    }

    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.trace("retrieveTrigger: {}", triggerKey);
        return couchbase.retrieveTrigger(triggerKey).orElse(null);
    }

    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        log.trace("checkExists: {}", jobKey);
        return couchbase.retrieveJob(jobKey).isPresent();
    }

    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        log.trace("checkExists: {}", triggerKey);
        return couchbase.retrieveTrigger(triggerKey).isPresent();
    }

    public void clearAllSchedulingData() throws JobPersistenceException {
        throw new UnsupportedOperationException("clearAllSchedulingData");
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
        throw new UnsupportedOperationException("getJobKeys");
    }

    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        throw new UnsupportedOperationException("getTriggerKeys");
    }

    public List<String> getJobGroupNames() throws JobPersistenceException {
        throw new UnsupportedOperationException("getJobGroupNames");
    }

    public List<String> getTriggerGroupNames() throws JobPersistenceException {
        throw new UnsupportedOperationException("getTriggerGroupNames");
    }

    public List<String> getCalendarNames() throws JobPersistenceException {
        throw new UnsupportedOperationException("getCalendarNames");
    }

    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        throw new UnsupportedOperationException("getTriggersForJob");
    }

    public Trigger.TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        throw new UnsupportedOperationException("getTriggerState");
    }

    public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
        throw new UnsupportedOperationException("resetTriggerFromErrorState");
    }

    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        throw new UnsupportedOperationException("pauseTrigger");
    }

    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        throw new UnsupportedOperationException("pauseTriggers");
    }

    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        throw new UnsupportedOperationException("pauseJob");
    }

    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        throw new UnsupportedOperationException("pauseJobs");
    }

    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        throw new UnsupportedOperationException("resumeTrigger");
    }

    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        throw new UnsupportedOperationException("resumeTriggers");
    }

    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        throw new UnsupportedOperationException("getPausedTriggerGroups");
    }

    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        throw new UnsupportedOperationException("resumeJob");
    }

    public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        throw new UnsupportedOperationException("resumeJobs");
    }

    public void pauseAll() throws JobPersistenceException {
        throw new UnsupportedOperationException("pauseAll");
    }

    public void resumeAll() throws JobPersistenceException {
        throw new UnsupportedOperationException("resumeAll");
    }

    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
            throws JobPersistenceException {
        log.trace("acquireNextTriggers: {}, {}, {}", noLaterThan, maxCount, timeWindow);

        List<OperableTrigger> triggers = new ArrayList<>();
        for (int tries = 0; tries < maxAcquireRetries && triggers.isEmpty(); tries++) {
            log.trace("Attempt {}/{} to acquire next triggers", tries, maxAcquireRetries);
            long maxNextFireTime = Math.max(noLaterThan, System.currentTimeMillis()) + timeWindow;

            List<TriggerKey> triggerKeys = couchbase.selectTriggerKeys(
                    instanceName, READY, maxCount, maxNextFireTime);
            log.debug("Trying to acquire {} triggers", triggerKeys.size());

            for (TriggerKey triggerKey : triggerKeys) {
                log.trace("Trying to acquire trigger: {}", triggerKey);
                couchbase.updateTriggerState(triggerKey, READY, ACQUIRED).ifPresent(trigger -> {
                    log.trace("Acquired trigger: {}", trigger.getKey());
                    triggers.add(trigger);
                });
            }
        }
        return triggers;
    }

    public void releaseAcquiredTrigger(OperableTrigger trigger) {
        throw new UnsupportedOperationException("releaseAcquiredTrigger");
    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        log.trace("triggersFired: {}", triggers);
        List<TriggerFiredResult> result = new ArrayList<>();
        for (OperableTrigger trigger : triggers) {
            Optional<JobDetail> triggerJob = couchbase.retrieveJob(trigger.getJobKey());
            if (triggerJob.isPresent()) {
                JobDetail job = triggerJob.get();
                Date scheduledFireTime = trigger.getPreviousFireTime();

                trigger.triggered(null);
                couchbase.storeTrigger(trigger, READY, true);

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
        log.trace("triggeredJobComplete: {}, {}, {}", trigger, jobDetail, instruction);
        retryWhileAvailable(() -> executeInLock("triggeredJobComplete", LOCK_TRIGGER_ACCESS, () -> {
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
        throw new UnsupportedOperationException("getAcquireRetryDelay");
    }

    private <T> T executeInLock(
            String lockerName, String lockName, PersistenceSupplier<T> action)
            throws JobPersistenceException {
        int tries = 0;
        do {
            tries++;
            try (AcquiredLock lock = couchbase.getLock(lockerName, lockName)) {
                log.debug("[{}] Acquired lock {}", lockerName, lockName);
                return action.get();
            }
            catch (LockException e) {
                if (e.isRetriable()) {
                    log.warn("[{}] Failed to acquire lock {} (attempt {}/{})",
                            lockerName, lockName, tries, maxLockRetries, e);
                    sleepQuietly(lockRetryDelay);
                }
                else {
                    log.error("[{}] Failed to acquire lock {}", lockerName, lockName, e);
                    throw e;
                }
            }
        }
        while (tries < maxLockRetries);

        log.error("[{}] Failed to acquire lock {} after {} attempts",
                lockerName, lockName, maxLockRetries);
        throw new JobPersistenceException(String.format("[%s] Failed to acquire lock %s after %d attempts.",
                lockerName, lockName, maxLockRetries));
    }

    private <T> T retryWhileAvailable(PersistenceSupplier<T> action) {
        while (!couchbase.isShutdown()) {
            try {
                return action.get();
            }
            catch (Exception e) {
                log.trace("Failed to execute, but insisting on it.", e);
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
