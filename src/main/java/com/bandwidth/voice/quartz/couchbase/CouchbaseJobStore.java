package com.bandwidth.voice.quartz.couchbase;

import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.allOf;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.jobId;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.lockId;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.sleepQuietly;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.triggerId;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static com.couchbase.client.java.query.dsl.Sort.asc;
import static com.couchbase.client.java.query.dsl.functions.AggregateFunctions.count;
import static com.couchbase.client.java.query.dsl.functions.DateFunctions.millis;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;

import com.bandwidth.voice.quartz.couchbase.converter.SimpleTriggerConverter;
import com.bandwidth.voice.quartz.couchbase.converter.TriggerConverter;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TemporaryLockFailureException;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Calendar;
import org.quartz.Job;
import org.quartz.JobDataMap;
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

    private Bucket bucket;

    private final Set<TriggerConverter<?>> triggerConverters = Set.of(
            new SimpleTriggerConverter());

    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        log.info("Initializing {} with bucket '{}' and password '{}'", getClass().getSimpleName(), bucketName,
                bucketPassword);
        this.bucket = Optional.ofNullable(bucketPassword)
                .map(password -> cluster.openBucket(bucketName, password))
                .orElseGet(() -> cluster.openBucket(bucketName));
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
        log.debug("storeJobAndTrigger: {}, {}", job, trigger);
        executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            storeJob(jobId(job.getKey()), convertJob(job));
            storeTrigger(triggerId(trigger.getKey()), convertTrigger(trigger));
            return null;
        }, () -> {
            removeJob(job.getKey());
            removeTrigger(trigger.getKey());
        });
    }

    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws JobPersistenceException {
        log.debug("storeJobsAndTriggers: {}, {}", triggersAndJobs, replace);
    }

    public void storeJob(JobDetail job, boolean replaceExisting) throws JobPersistenceException {
        log.debug("storeJob: {}, {}", job, replaceExisting);
        executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            storeJob(jobId(job.getKey()), convertJob(job));
            return null;
        });
    }

    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        log.debug("removeJob: {}", jobKey);

        return false;
    }

    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        log.debug("removeJobs: {}", jobKeys);
        return false;
    }

    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        log.debug("retrieveJob: {}", jobKey);
        return Optional.ofNullable(bucket.get(jobId(jobKey)))
                .map(document -> convertJob(document.content()))
                .orElse(null);
    }

    public void storeTrigger(OperableTrigger trigger, boolean replaceExisting) throws JobPersistenceException {
        log.debug("storeTrigger: {}, {}", trigger, replaceExisting);
        executeInLock(LOCK_TRIGGER_ACCESS, () -> {
            storeTrigger(triggerId(trigger.getKey()), convertTrigger(trigger));
            return null;
        });
    }

    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("removeTrigger: {}", triggerKey);
        return false;
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        log.debug("removeTriggers: {}", triggerKeys);
        return false;
    }

    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        log.debug("replaceTrigger: {}, {}", triggerKey, newTrigger);
        return false;
    }

    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("retrieveTrigger: {}", triggerKey);
        return null;
    }

    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        log.debug("checkExists: {}", jobKey);
        return false;
    }

    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        log.debug("checkExists: {}", triggerKey);
        return false;
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
        for (int tries = 0; tries < MAX_ACQUIRE_TRIES; tries++) {
            long maxNextFireTime = Math.max(noLaterThan, System.currentTimeMillis()) + timeWindow;
            N1qlQueryResult result = query(select("META().id").from(bucketName)
                    .where(CouchbaseUtils.allOf(
                            x("META().id").like(s("T.%")),
                            i("schedulerName").eq(s(instanceName)),
                            i("state").eq(s("READY")),
                            millis(i("nextFireTime")).lte(maxNextFireTime)))
                    .orderBy(asc(i("nextFireTime")))
                    .limit(maxCount));

            List<OperableTrigger> triggers = result.allRows().stream()
                    .map(row -> row.value().getString("id"))
                    .flatMap(triggerId -> updateTriggerStatus(triggerId, "READY", "ACQUIRED").stream())
                    .collect(toList());

            if (!triggers.isEmpty()) {
                return triggers;
            }
        }
        return List.of();
    }

    public void releaseAcquiredTrigger(OperableTrigger trigger) {
        log.debug("releaseAcquiredTrigger: {}", trigger);
    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        log.debug("triggersFired: {}", triggers);
        List<TriggerFiredResult> result = new ArrayList<>();
        for (OperableTrigger trigger : triggers) {
            JobDetail job = retrieveJob(trigger.getJobKey());
            trigger.triggered(null);
            TriggerFiredBundle bundle = new TriggerFiredBundle(job, trigger, null, false,
                    new Date(), trigger.getNextFireTime(), trigger.getPreviousFireTime(), trigger.getNextFireTime());
            result.add(new TriggerFiredResult(bundle));
        }
        return result;
    }

    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
            CompletedExecutionInstruction instruction) {
        log.debug("triggeredJobComplete: {}, {}, {}", trigger, jobDetail, instruction);
        retryWhileAvailable(() -> {
            String triggerId = triggerId(trigger.getKey());
            switch (instruction) {
                case SET_TRIGGER_COMPLETE:
                    updateTriggerStatus(triggerId, "COMPLETE");
                    break;
                case SET_TRIGGER_ERROR:
                    updateTriggerStatus(triggerId, "ERROR");
                    break;
                case DELETE_TRIGGER:
                    removeTrigger(triggerId);
                    break;
                case SET_ALL_JOB_TRIGGERS_COMPLETE:
                case SET_ALL_JOB_TRIGGERS_ERROR:
                    log.warn("Not supported: {}", instruction);
                    break;
            }
            return null;
        });
    }

    public long getAcquireRetryDelay(int failureCount) {
        return 0;
    }

    /*
     * Couchbase specific
     */

    private void storeJob(String jobId, JsonObject job) throws JobPersistenceException {
        JsonDocument document = JsonDocument.create(jobId, job);
        try {
            bucket.insert(document);
        }
        catch (DocumentAlreadyExistsException e) {
            throw new ObjectAlreadyExistsException("Job already exists: " + jobId);
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Error persisting job", e);
        }
    }

    private Optional<JsonDocument> retrieveJob(String jobId) {
        return Optional.ofNullable(bucket.get(jobId));
    }

    private Optional<JsonDocument> retrieveTriggerJob(String triggerId) {
        return retrieveTrigger(triggerId)
                .map(trigger -> jobKey(
                        trigger.content().getString("jobName"),
                        trigger.content().getString("jobGroup")))
                .flatMap(jobKey -> retrieveJob(jobId(jobKey)));
    }

    private void removeJob(String jobId) {
        try {
            bucket.remove(jobId);
        }
        catch (DocumentDoesNotExistException e) {
            // Do nothing
        }
    }

    private void storeTrigger(String triggerId, JsonObject trigger) throws JobPersistenceException {
        JsonDocument document = JsonDocument.create(
                triggerId,
                trigger.put("state", "READY"));
        try {
            bucket.insert(document);
        }
        catch (DocumentAlreadyExistsException e) {
            throw new ObjectAlreadyExistsException("Trigger already exists: " + triggerId);
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Error persisting trigger", e);
        }
    }

    private Optional<JsonDocument> retrieveTrigger(String triggerId) {
        return Optional.ofNullable(bucket.get(triggerId));
    }

    private Optional<OperableTrigger> updateTriggerStatus(String triggerId, String expectedStatus, String newStatus) {
        return Optional.ofNullable(bucket.get(triggerId))
                .filter(document -> Objects.equals(document.content().getString("state"), expectedStatus))
                .flatMap(document -> {
                    document.content().put("state", newStatus);
                    return tryReplace(document);
                })
                .map(document -> convertTrigger(document.content()));
    }

    private Optional<OperableTrigger> updateTriggerStatus(String triggerId, String newStatus) {
        return Optional.ofNullable(bucket.get(triggerId))
                .flatMap(document -> {
                    document.content().put("state", newStatus);
                    return tryReplace(document);
                })
                .map(document -> convertTrigger(document.content()));
    }

    private Optional<JsonDocument> tryReplace(JsonDocument document) {
        try {
            return Optional.of(bucket.replace(document));
        }
        catch (CASMismatchException e) {
            log.debug("Document {} was modified", document.id());
            return Optional.empty();
        }
        catch (DocumentDoesNotExistException e) {
            log.debug("Document {} was removed", document.id());
            return Optional.empty();
        }
    }

    private void removeTrigger(String triggerId) throws JobPersistenceException {
        Optional<JsonDocument> triggerJob = retrieveTriggerJob(triggerId)
                .filter(job -> !job.content().getBoolean("isDurable"));

        try {
            bucket.remove(triggerId);
        }
        catch (DocumentDoesNotExistException e) {
            // Trigger didn't exist, so obviously no associated job either.
            // Get back to work then.
            return;
        }

        if (triggerJob.isPresent()) {
            // TODO: improve this mess
            JsonObject job = triggerJob.get().content();
            N1qlQueryResult result = query(select(count("*").as("count"))
                    .from(bucketName)
                    .where(allOf(
                            i("jobName").eq( s(job.getString("name"))),
                            i("jobGroup").eq(s(job.getString("group"))))));
            Integer count = result.allRows().get(0).value().getInt("count");
            if (count == 0) {
                removeJob(triggerJob.get().id());
            }
        }
    }

    private JsonObject convertJob(JobDetail job) {
        return JsonObject.create()
                .put("name", job.getKey().getName())
                .put("group", job.getKey().getGroup())
                .put("description", job.getDescription())
                .put("type", job.getJobClass().getName())
                .put("data", job.getJobDataMap())
                .put("isDurable", job.isDurable())
                .put("isRecoverable", job.requestsRecovery());
    }

    @SuppressWarnings("unchecked")
    private JobDetail convertJob(JsonObject object) {
        try {
            return newJob()
                    .withIdentity(object.getString("name"), object.getString("group"))
                    .withDescription(object.getString("description"))
                    .ofType((Class<? extends Job>) Class.forName(object.getString("type")))
                    .usingJobData(new JobDataMap(object.getObject("data").toMap()))
                    .storeDurably(object.getBoolean("isDurable"))
                    .requestRecovery(object.getBoolean("isRecoverable"))
                    .build();
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Unknown job type: " + object.getString("type"));
        }
    }

    private JsonObject convertTrigger(OperableTrigger trigger) {
        return triggerConverters.stream()
                .flatMap(e -> e.cast(trigger).stream())
                .findAny().orElseThrow()
                .convert(trigger)
                .put("schedulerName", instanceName);
    }

    private OperableTrigger convertTrigger(JsonObject object) {
        return triggerConverters.stream()
                .flatMap(e -> e.cast(object).stream())
                .findAny().orElseThrow()
                .convert(object);
    }

    private N1qlQueryResult query(Statement query) throws JobPersistenceException {
        log.debug("Query: {}", query);
        N1qlQueryResult result = bucket.query(query);
        if (!result.finalSuccess()) {
            throw new JobPersistenceException("Failed to query triggers: " + result.errors());
        }
        return result;
    }

    private <T> T executeInLock(String lockName, PersistenceSupplier<T> action) throws JobPersistenceException {
        return executeInLock(lockName, action, () -> { });
    }

    private <T> T executeInLock(String lockName, PersistenceSupplier<T> action, PersistenceRunnable rollback)
            throws JobPersistenceException {
        int tries = 0;
        String lockId = lockId(instanceName, lockName);

        do {
            tries++;
            try (AcquiredLock lock = new AcquiredLock(lockId)) {
                return action.get();
            }
            catch (LockException e) {
                if (e.isRetriable()) {
                    log.warn("Failed to acquire lock {} (attempt {}/{})", lockId, tries, MAX_LOCK_TRIES, e);
                    sleepQuietly(100);
                }
                else {
                    log.error("Failed to acquire lock {}", lockId, e);
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
        throw new JobPersistenceException(
                String.format("Failed to acquire lock %s after %d attempts.", lockId, MAX_LOCK_TRIES));
    }

    private <T> T retryWhileAvailable(PersistenceSupplier<T> action) {
        while (!bucket.isClosed()) {
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

    private interface PersistenceSupplier<T> {
        T get() throws JobPersistenceException;
    }

    private interface PersistenceRunnable {
        void run() throws JobPersistenceException;
    }

}
