package com.bandwidth.voice.quartz.couchbase.store;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.functions.AggregateFunctions.count;
import static org.quartz.JobBuilder.newJob;

import com.bandwidth.voice.quartz.couchbase.TriggerState;
import com.bandwidth.voice.quartz.couchbase.converter.SimpleTriggerConverter;
import com.bandwidth.voice.quartz.couchbase.converter.TriggerConverter;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import java.util.Set;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.spi.OperableTrigger;

@Slf4j
@Builder
public class DenormalizedCouchbaseDelegate {

    @NonNull
    private final String schedulerName;
    @NonNull
    private final Bucket bucket;

    private final Set<TriggerConverter<?>> triggerConverters = Set.of(
            new SimpleTriggerConverter());

    public void shutdown() {
        if (bucket != null && !bucket.isClosed()) {
            bucket.close();
        }
    }

    public boolean isShutdown() {
        return bucket.isClosed();
    }

    public void storeJobWithTrigger(JobDetail job, OperableTrigger trigger) {
        String jobId = job.getKey().toString();
        JsonDocument document = bucket.get(jobId);
        if (document == null) {
            bucket.insert(JsonDocument.create(jobId, convertJob(job)
                    .put("triggers", JsonArray.from(convertTrigger(trigger)))));
        }
        else {
            document.content().getArray("triggers").add(convertTrigger(trigger));
            bucket.replace(document);
        }
    }

    private void insertOrUpsert(boolean upsert, JsonDocument document) {
        if (upsert) {
            bucket.upsert(document);
        }
        else {
            bucket.insert(document);
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

    private TriggerState getTriggerState(JsonObject trigger) {
        return TriggerState.valueOf(trigger.getString("state"));
    }

    private JsonObject setTriggerState(JsonObject trigger, TriggerState state) {
        return trigger.put("state", state.name());
    }

    private N1qlQueryResult query(Statement query) throws JobPersistenceException {
        log.trace("Query: {}", query);

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

    private int countAll(Expression filter) throws JobPersistenceException {
        log.trace("Count: {}", filter);

        try {
            N1qlQueryResult result = bucket.query(select(count("*").as("count"))
                    .from(bucket.name())
                    .where(filter));
            if (!result.finalSuccess()) {
                throw new JobPersistenceException("Count failed: " + result.errors());
            }
            return result.allRows().get(0).value().getInt("count");
        }
        catch (CouchbaseException e) {
            throw new JobPersistenceException("Failed to execute count", e);
        }
    }
}
