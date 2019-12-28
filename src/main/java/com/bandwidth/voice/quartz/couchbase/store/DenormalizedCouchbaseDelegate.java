package com.bandwidth.voice.quartz.couchbase.store;

import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.allOf;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.e;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.ipath;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.functions.DateFunctions.millis;
import static java.text.MessageFormat.format;
import static java.util.stream.Collectors.toList;
import static org.quartz.JobKey.jobKey;

import com.bandwidth.voice.quartz.couchbase.LockException;
import com.bandwidth.voice.quartz.couchbase.TriggerState;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.message.kv.subdoc.multi.Lookup;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.subdoc.MutateInBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

@Slf4j
public class DenormalizedCouchbaseDelegate extends CouchbaseDelegate {

    @Builder
    public DenormalizedCouchbaseDelegate(Bucket bucket, String schedulerName) {
        super(bucket, schedulerName);
    }

    public void storeJobWithTriggers(
            JobDetail job, Set<? extends Trigger> triggers, TriggerState state, boolean replace)
            throws JobPersistenceException {
        try {
            insertOrUpsert(replace, JsonDocument.create(
                    jobId(job.getKey()),
                    convertJob(job).put("triggers", JsonArray.from(triggers.stream()
                            .map(t -> convertTrigger((OperableTrigger) t).put("state", state.toString()))
                            .collect(toList())))));
        }
        catch (DocumentAlreadyExistsException e) {
            throw new ObjectAlreadyExistsException(job);
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to store job: " + job.getKey(), e);
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

    public void storeTrigger(OperableTrigger trigger, TriggerState state, boolean replace)
            throws JobPersistenceException {
        String jobId = jobId(trigger.getJobKey());
        DocumentFragment<Lookup> document;
        try {
            document = bucket.lookupIn(jobId)
                    .get("triggers")
                    .execute();
        }
        catch (DocumentDoesNotExistException e) {
            throw new JobPersistenceException("Job not found: " + trigger.getJobKey(), e);
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to retrieve job: " + trigger.getJobKey(), e);
        }

        MutateInBuilder mutation = bucket.mutateIn(jobId)
                .withCas(document.cas())
                .arrayAppend("triggers", convertTrigger(trigger).put("state", state.toString()));

        Optional<Integer> existing = findTriggerIndex(trigger.getKey(), document.content("triggers", JsonArray.class));
        if (existing.isPresent()) {
            if (!replace) {
                throw new ObjectAlreadyExistsException(trigger);
            }
            mutation.remove(format("triggers[{0}]", existing.get()));
        }

        try {
            mutation.execute();
        }
        catch (CASMismatchException | DocumentDoesNotExistException e) {
            throw new LockException(true, "Failed to store trigger (was modified): " + trigger.getKey(), e);
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to store trigger: " + trigger.getKey(), e);
        }
    }

    public ListMultimap<JobKey, TriggerKey> selectTriggerKeys(TriggerState state, long maxNextFireTime, int maxCount)
            throws JobPersistenceException {
        N1qlQueryResult result = query(select(
                ipath("job", "name").as("jobName"),
                ipath("job", "group").as("jobGroup"),
                ipath("triggers", "name"),
                ipath("triggers", "group"))
                .from(bucket.name() + " job")
                .unnest("triggers")
                .where(allOf(
                        ipath("triggers", "schedulerName").eq(s(schedulerName)),
                        ipath("triggers", "state").eq(e(state)),
                        millis(ipath("triggers", "nextFireTime")).lte(maxNextFireTime)))
                .limit(maxCount));

        return result.allRows().stream()
                .map(N1qlQueryRow::value)
                .collect(ArrayListMultimap::create,
                        (map, row) -> map.put(
                                jobKey(row.getString("jobName"), row.getString("jobGroup")),
                                triggerKey(row)),
                        ArrayListMultimap::putAll);
    }

    public Optional<OperableTrigger> updateTriggerState(
            JobKey jobKey, TriggerKey triggerKey, TriggerState from, TriggerState to)
            throws JobPersistenceException {
        try {
            JsonDocument document = bucket.get(jobId(jobKey));
            if (document == null) {
                return Optional.empty();
            }

            Optional<OperableTrigger> trigger = findTrigger(triggerKey, document.content().getArray("triggers"))
                    .filter(t -> TriggerState.valueOf(t.getString("state")) == from)
                    // Updating the trigger in-place so we don't have to reconstruct the JsonArray.
                    .map(t -> t.put("state", to.toString()))
                    .map(object -> convertTrigger(jobKey, object));

            trigger.ifPresent(x -> bucket.replace(document));
            return trigger;
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to update trigger state: " + triggerKey);
        }
    }

    @Override
    protected JsonObject convertTrigger(OperableTrigger trigger) {
        return super.convertTrigger(trigger)
                .removeKey("jobName")
                .removeKey("jobGroup");
    }

    private static Optional<Integer> findTriggerIndex(TriggerKey triggerKey, JsonArray triggers) {
        return IntStream.range(0, triggers.size()).boxed()
                .filter(i -> triggerKey(triggers.getObject(i)).equals(triggerKey))
                .findFirst();
    }

    private static Optional<JsonObject> findTrigger(TriggerKey triggerKey, JsonArray triggers) {
        return findTriggerIndex(triggerKey, triggers)
                .map(triggers::getObject);
    }

    private static TriggerKey triggerKey(JsonObject row) {
        return TriggerKey.triggerKey(row.getString("name"), row.getString("group"));
    }

    private static String jobId(JobKey key) {
        return Objects.toString(key);
    }
}
