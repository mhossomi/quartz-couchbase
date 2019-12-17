package com.bandwidth.voice.quartz.couchbase;

import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.allOf;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.jobId;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.lockId;
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
import static org.quartz.TriggerKey.triggerKey;

import com.bandwidth.voice.quartz.couchbase.converter.SimpleTriggerConverter;
import com.bandwidth.voice.quartz.couchbase.converter.TriggerConverter;
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
import com.couchbase.client.java.query.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

@Slf4j
@RequiredArgsConstructor
public class CouchbaseDelegate {

    private final String schedulerName;
    private final Bucket bucket;

    private final Set<TriggerConverter<?>> triggerConverters = Set.of(
            new SimpleTriggerConverter());

    public void shutdown() {
        if (bucket != null && !bucket.isClosed()) {
            bucket.close();
        }
    }

    public boolean isAvailable() {
        return !bucket.isClosed();
    }

    public AcquiredLock getLock(String lockName) {
        return new AcquiredLock(lockId(schedulerName, lockName));
    }

    public void storeJob(JobDetail job) throws JobPersistenceException {
        try {
            bucket.insert(JsonDocument.create(jobId(job.getKey()), convertJob(job)));
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
            return true;
        }
        catch (DocumentDoesNotExistException e) {
            return false;
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to remove job: " + jobKey, e);
        }
    }

    public void storeTrigger(OperableTrigger trigger) throws JobPersistenceException {
        JsonDocument document = JsonDocument.create(
                triggerId(trigger.getKey()),
                convertTrigger(trigger).put("state", "READY"));
        try {
            bucket.insert(document);
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

    public List<TriggerKey> selectTriggerKeys(String instanceName, String state, int maxCount, long maxNextFireTime)
            throws JobPersistenceException {

        N1qlQueryResult result = query(select(i("name"), i("group"))
                .from(bucket.name())
                .where(allOf(
                        x("META().id").like(s("T.%")),
                        i("schedulerName").eq(s(instanceName)),
                        i("state").eq(s("READY")),
                        millis(i("nextFireTime")).lte(maxNextFireTime)))
                .orderBy(asc(i("nextFireTime")))
                .limit(maxCount));

        return result.allRows().stream()
                .map(N1qlQueryRow::value)
                .map(row -> triggerKey(row.getString("name"), row.getString("group")))
                .collect(toList());
    }

    public Optional<OperableTrigger> updateTriggerStatus(TriggerKey triggerKey, String from, String to)
            throws JobPersistenceException {
        try {
            return Optional.of(triggerId(triggerKey))
                    .map(bucket::get)
                    .filter(document -> from == null || Objects.equals(from, document.content().getString("state")))
                    .map(document -> {
                        String previousState = document.content().getString("state");
                        document.content().put("state", to);
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

    public Optional<OperableTrigger> updateTriggerStatus(TriggerKey triggerKey, String newStatus)
            throws JobPersistenceException {
        return updateTriggerStatus(triggerKey, null, newStatus);
    }

    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        Optional<JobKey> triggerJobKey = retrieveTriggerJob(triggerKey)
                .filter(job -> !job.isDurable())
                .map(JobDetail::getKey);

        try {
            bucket.remove(triggerId(triggerKey));
        }
        catch (DocumentDoesNotExistException e) {
            // Trigger didn't exist, so obviously no associated job either.
            // Get back to work then.
            return false;
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to remove trigger", e);
        }

        if (triggerJobKey.isPresent()) {
            JobKey jobKey = triggerJobKey.get();
            N1qlQueryResult result = query(select(count("*").as("count"))
                    .from(bucket.name())
                    .where(allOf(
                            i("jobName").eq(s(jobKey.getName())),
                            i("jobGroup").eq(s(jobKey.getName())))));

            int count = result.allRows().get(0).value().getInt("count");
            log.debug("Job {} has {} triggers", jobKey, count);

            if (count == 0) {
                removeJob(jobKey);
            }
        }
        return true;
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
        String type = object.getString("type");
        try {
            return newJob()
                    .withIdentity(object.getString("name"), object.getString("group"))
                    .withDescription(object.getString("description"))
                    .ofType((Class<? extends Job>) Class.forName(type))
                    .usingJobData(new JobDataMap(object.getObject("data").toMap()))
                    .storeDurably(object.getBoolean("isDurable"))
                    .requestRecovery(object.getBoolean("isRecoverable"))
                    .build();
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Unknown job type: " + type);
        }
    }

    private JsonObject convertTrigger(OperableTrigger trigger) {
        Class<? extends OperableTrigger> type = trigger.getClass();
        return triggerConverters.stream()
                .flatMap(e -> e.forType(type).stream())
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("No converter found for trigger type " + type))
                .convert(trigger)
                .put("schedulerName", schedulerName);
    }

    private OperableTrigger convertTrigger(JsonObject object) {
        String type = object.getString("type");
        return triggerConverters.stream()
                .flatMap(e -> e.forType(type).stream())
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("No converter found for trigger type " + type))
                .convert(object);
    }

    private N1qlQueryResult query(Statement query) throws JobPersistenceException {
        log.debug("Query: {}", query);

        try {
            N1qlQueryResult result = bucket.query(query);
            if (!result.finalSuccess()) {
                throw new JobPersistenceException("Query failed: " + result.errors());
            }
            return result;
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to execute query", e);
        }

    }

    @RequiredArgsConstructor
    public class AcquiredLock implements AutoCloseable {

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
                log.debug("Lock {} acquired", lock.id());
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
}
