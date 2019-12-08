package com.bandwidth.voice.quartz.couchbase;

import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.jobId;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.lockId;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.sleepQuietly;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.triggerId;
import static java.lang.String.format;

import com.bandwidth.voice.quartz.couchbase.converter.SimpleTriggerConverter;
import com.bandwidth.voice.quartz.couchbase.converter.TriggerConverter;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.TemporaryLockFailureException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
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
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;

@Slf4j
@NoArgsConstructor
public class CouchbaseJobStore implements JobStore {

    public static final int MAX_TRIES = 3;
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

    private Bucket bucket;
    private SchedulerSignaler signaler;

    private final Set<TriggerConverter<?>> triggerConverters = Set.of(
            new SimpleTriggerConverter());

    private JsonObject convert(JobDetail job) {
        return JsonObject.create()
                .put("id", jobId(job))
                .put("name", job.getKey().getName())
                .put("group", job.getKey().getGroup())
                .put("description", job.getDescription())
                .put("type", job.getJobClass().getName())
                .put("data", job.getJobDataMap());
    }

    private JsonObject convert(Trigger trigger) {
        return triggerConverters.stream()
                .flatMap(e -> e.cast(trigger).stream())
                .findAny().orElseThrow()
                .convert(trigger);
    }

    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        log.info("Initializing {} with bucket '{}' and password '{}'", getClass().getSimpleName(), bucketName,
                bucketPassword);
        this.bucket = Optional.ofNullable(bucketPassword)
                .map(password -> cluster.openBucket(bucketName, password))
                .orElseGet(() -> cluster.openBucket(bucketName));
        this.signaler = signaler;
    }

    public void schedulerStarted() throws SchedulerException {

    }

    public void schedulerPaused() {

    }

    public void schedulerResumed() {

    }

    public void shutdown() {
        if (bucket != null && !bucket.isClosed()) {
            bucket.close();
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
        executeInLock("trigger-access", () -> {
            JsonDocument document = JsonDocument.create(
                    triggerId(trigger),
                    convert(trigger).put("jobs", JsonArray.from(convert(job))));
            try {
                // TODO: check job exists within triggers
                bucket.insert(document);
            }
            catch (DocumentAlreadyExistsException e) {
                throw new ObjectAlreadyExistsException(trigger);
            }
            catch (CouchbaseException e) {
                throw new JobPersistenceException("Error persisting trigger", e);
            }
        });
    }

    public void storeJob(JobDetail job, boolean replaceExisting) throws JobPersistenceException {
        executeInLock("trigger-access", () -> {
            JsonDocument document = JsonDocument.create(
                    jobId(job),
                    convert(job));
            try {
                // TODO: check job exists within triggers
                bucket.insert(document);
            }
            catch (DocumentAlreadyExistsException e) {
                throw new ObjectAlreadyExistsException(job);
            }
            catch (CouchbaseException e) {
                throw new JobPersistenceException("Error persisting job", e);
            }
        });
    }

    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws JobPersistenceException {
        executeInLock("trigger-access", () -> {
            for (var e : triggersAndJobs.entrySet()) {
                JobDetail job = e.getKey();
                for (Trigger trigger : e.getValue()) {
                    storeJobAndTrigger(job, (OperableTrigger) trigger);
                }
            }
        });
    }

    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        return false;
    }

    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        return false;
    }

    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return null;
    }

    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
            throws ObjectAlreadyExistsException, JobPersistenceException {

    }

    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return false;
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        return false;
    }

    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        return false;
    }

    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return null;
    }

    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return false;
    }

    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        return false;
    }

    public void clearAllSchedulingData() throws JobPersistenceException {

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
        return null;
    }

    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
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
        return null;
    }

    public Trigger.TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        return null;
    }

    public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {

    }

    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {

    }

    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return null;
    }

    public void pauseJob(JobKey jobKey) throws JobPersistenceException {

    }

    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        return null;
    }

    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {

    }

    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return null;
    }

    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        return null;
    }

    public void resumeJob(JobKey jobKey) throws JobPersistenceException {

    }

    public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        return null;
    }

    public void pauseAll() throws JobPersistenceException {

    }

    public void resumeAll() throws JobPersistenceException {

    }

    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
            throws JobPersistenceException {
        return null;
    }

    public void releaseAcquiredTrigger(OperableTrigger trigger) {

    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        return null;
    }

    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
            Trigger.CompletedExecutionInstruction triggerInstCode) {

    }

    public long getAcquireRetryDelay(int failureCount) {
        return 0;
    }

    private void executeInLock(String lockName, PersistenceRunnable action) throws JobPersistenceException {
        int tries = 0;
        String lockId = lockId(instanceName, lockName);

        do {
            tries++;
            try (AcquiredLock lock = new AcquiredLock(lockId)) {
                action.run();
                return;
            }
            catch (LockException e) {
                if (e.isRetriable()) {
                    log.warn("Failed to acquire lock {} (attempt {}/{})", lockId, tries, MAX_TRIES, e);
                    sleepQuietly(100);
                }
                else {
                    log.error("Failed to acquire lock {}", lockId, e);
                    throw e;
                }
            }
        }
        while (tries < MAX_TRIES);

        throw new JobPersistenceException(
                String.format("Failed to acquire lock %s after %d attempts.", lockId, MAX_TRIES));
    }

    @RequiredArgsConstructor
    private class AcquiredLock implements AutoCloseable {

        private final Document<?> lock;

        public AcquiredLock(String lockId) {
            Document<?> newLock = JsonDocument.create(lockId);

            try {
                Document<?> acquiredLock = bucket.getAndLock(newLock, 5);
                if (acquiredLock == null) {
                    bucket.insert(newLock);
                    lock = bucket.getAndLock(newLock, 5);
                }
                else {
                    lock = acquiredLock;
                }
                log.info("Lock {} acquired", lock.id());
            }
            catch (TemporaryLockFailureException | DocumentAlreadyExistsException e) {
                throw new LockException(true, format("Lock %s unavailable", newLock.id()));
            }
            catch (Exception e) {
                throw new LockException(false, format("Failed to acquire lock %s", newLock.id()), e);
            }
        }

        @Override
        public void close() {
            if (lock != null) {
                log.info("Releasing lock {}", lock.id());
                bucket.unlock(lock.id(), lock.cas());
            }
        }
    }

    private interface PersistenceRunnable {
        void run() throws JobPersistenceException;
    }
}
