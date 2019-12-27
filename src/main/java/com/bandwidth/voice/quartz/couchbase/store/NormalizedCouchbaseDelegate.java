package com.bandwidth.voice.quartz.couchbase.store;

import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.allOf;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.e;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.jobId;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.lockId;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.triggerId;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static com.couchbase.client.java.query.dsl.Sort.asc;
import static com.couchbase.client.java.query.dsl.functions.DateFunctions.millis;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.quartz.TriggerKey.triggerKey;

import com.bandwidth.voice.quartz.couchbase.CouchbaseUtils;
import com.bandwidth.voice.quartz.couchbase.LockException;
import com.bandwidth.voice.quartz.couchbase.TriggerState;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TemporaryLockFailureException;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

@Slf4j
public class NormalizedCouchbaseDelegate extends CouchbaseDelegate {

    @NonNull
    private final Integer maxLockTime;

    @Builder
    public NormalizedCouchbaseDelegate(Bucket bucket, String schedulerName,
            Integer maxLockTime) {
        super(bucket, schedulerName);
        this.maxLockTime = maxLockTime;
    }

    public AcquiredLock getLock(String lockerName, String lockName) {
        return new AcquiredLock(lockerName, lockId(schedulerName, lockName));
    }

    public void storeJob(JobDetail job, boolean replaceExisting) throws JobPersistenceException {
        try {
            insertOrUpsert(replaceExisting, JsonDocument.create(
                    jobId(job.getKey()),
                    convertJob(job)));
            log.debug("Stored job {}", job.getKey());
        }
        catch (DocumentAlreadyExistsException e) {
            throw new ObjectAlreadyExistsException("Job already exists: " + job.getKey());
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to persist job: " + job.getKey(), e);
        }
    }

    public Optional<JobDetail> retrieveJob(JobKey jobKey) throws JobPersistenceException {
        try {
            return Optional.of(jobId(jobKey))
                    .map(bucket::get)
                    .map(document -> convertJob(document.content()));
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to retrieve job: " + jobKey, e);
        }
    }

    public Optional<JobDetail> retrieveTriggerJob(TriggerKey triggerKey) throws JobPersistenceException {
        try {
            return retrieveTrigger(triggerKey)
                    .map(Trigger::getJobKey)
                    .map(CouchbaseUtils::jobId)
                    .map(bucket::get)
                    .map(document -> convertJob(document.content()));
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to retrieve job for trigger: " + triggerKey, e);
        }
    }

    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        try {
            bucket.remove(jobId(jobKey));
            log.debug("Removed job {}", jobKey);
            return true;
        }
        catch (DocumentDoesNotExistException e) {
            log.debug("Job {} didn't exist", jobKey);
            return false;
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to remove job: " + jobKey, e);
        }
    }

    public void storeTrigger(OperableTrigger trigger, TriggerState state, boolean replaceExisting)
            throws JobPersistenceException {
        try {
            insertOrUpsert(replaceExisting, JsonDocument.create(
                    triggerId(trigger.getKey()),
                    convertTrigger(trigger).put("state", state.toString())));
            log.debug("Stored trigger {} with state {}", trigger.getKey(), state);
        }
        catch (DocumentAlreadyExistsException e) {
            throw new ObjectAlreadyExistsException("Trigger already exists: " + trigger.getKey());
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to persist trigger: " + trigger.getKey(), e);
        }
    }

    public Optional<OperableTrigger> retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        try {
            return Optional.of(triggerId(triggerKey))
                    .map(bucket::get)
                    .map(document -> convertTrigger(document.content()));
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to retrieve trigger: " + triggerKey, e);
        }
    }

    public List<TriggerKey> selectTriggerKeys(String instanceName, TriggerState state, int maxCount,
            long maxNextFireTime)
            throws JobPersistenceException {

        N1qlQueryResult result = query(select(i("name"), i("group"))
                .from(bucket.name())
                .where(allOf(
                        i("schedulerName").eq(s(instanceName)),
                        x("META().id").like(s("T.%")),
                        i("state").eq(e(state)),
                        millis(i("nextFireTime")).lte(maxNextFireTime)))
                .orderBy(asc(i("nextFireTime")))
                .limit(maxCount));

        return result.allRows().stream()
                .map(N1qlQueryRow::value)
                .map(row -> triggerKey(row.getString("name"), row.getString("group")))
                .collect(toList());
    }

    public Optional<OperableTrigger> updateTriggerState(TriggerKey triggerKey, TriggerState from, TriggerState to)
            throws JobPersistenceException {
        try {
            return Optional.of(triggerId(triggerKey))
                    .map(bucket::get)
                    .filter(document -> from == null || from == getTriggerState(document.content()))
                    .map(document -> {
                        TriggerState previousState = getTriggerState(document.content());
                        setTriggerState(document.content(), to);
                        try {
                            JsonDocument replaced = bucket.replace(document);
                            log.info("Updated trigger {} state from {} to {}",
                                    triggerKey, previousState, to);
                            return replaced;
                        }
                        catch (CASMismatchException e) {
                            log.debug("Document {} was modified", document.id());
                            return null;
                        }
                        catch (DocumentDoesNotExistException e) {
                            log.debug("Document {} was removed", document.id());
                            return null;
                        }
                    })
                    .map(document -> convertTrigger(document.content()));
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to update trigger status: " + triggerKey, e);
        }
    }

    public Optional<OperableTrigger> updateTriggerState(TriggerKey triggerKey, TriggerState to)
            throws JobPersistenceException {
        return updateTriggerState(triggerKey, null, to);
    }

    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        Optional<JobKey> triggerJobKey = retrieveTriggerJob(triggerKey)
                .filter(job -> !job.isDurable())
                .map(JobDetail::getKey);

        try {
            bucket.remove(triggerId(triggerKey));
            log.debug("Removed trigger {}", triggerKey);
        }
        catch (DocumentDoesNotExistException e) {
            // Trigger didn't exist, so obviously no associated job either.
            // Get back to work then.
            log.debug("Trigger {} didn't exist", triggerKey);
            return false;
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to remove trigger", e);
        }

        if (triggerJobKey.isPresent()) {
            JobKey jobKey = triggerJobKey.get();
            int count = countAll(allOf(
                    i("jobName").eq(s(jobKey.getName())),
                    i("jobGroup").eq(s(jobKey.getGroup()))));
            log.debug("Job {} has {} triggers", jobKey, count);

            if (count == 0) {
                removeJob(jobKey);
            }
        }
        return true;
    }

    private void insertOrUpsert(boolean upsert, JsonDocument document) {
        if (upsert) {
            bucket.upsert(document);
        }
        else {
            bucket.insert(document);
        }
    }

    private TriggerState getTriggerState(JsonObject trigger) {
        return TriggerState.valueOf(trigger.getString("state"));
    }

    private JsonObject setTriggerState(JsonObject trigger, TriggerState state) {
        return trigger.put("state", state.name());
    }

    @RequiredArgsConstructor
    public class AcquiredLock implements AutoCloseable {

        private final String lockerName;
        private final Document<?> lock;

        public AcquiredLock(String lockerName, String lockId) {
            this.lockerName = lockerName;
            Document<?> newLock = JsonDocument.create(lockId);

            try {
                Document<?> acquiredLock = bucket.getAndLock(newLock, maxLockTime);
                if (acquiredLock == null) {
                    bucket.insert(newLock);
                    lock = bucket.getAndLock(newLock, maxLockTime);
                }
                else {
                    lock = acquiredLock;
                }
                log.debug("[{}] Lock {} acquired", lockerName, lock.id());
            }
            catch (TemporaryLockFailureException | DocumentAlreadyExistsException e) {
                throw new LockException(true, format("[%s] Lock %s unavailable", lockerName, newLock.id()));
            }
            catch (Exception e) {
                throw new LockException(false, format("[%s] Failed to acquire lock %s", lockerName, newLock.id()), e);
            }
        }

        @Override
        public void close() {
            if (lock != null) {
                log.info("[{}] Releasing lock {}", lockerName, lock.id());
                bucket.unlock(lock.id(), lock.cas());
            }
        }
    }
}
