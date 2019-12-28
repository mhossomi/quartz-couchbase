package com.bandwidth.voice.quartz.couchbase.store;

import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.allOf;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.e;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.ipath;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.READY;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.functions.DateFunctions.millis;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerKey.triggerKey;

import com.bandwidth.voice.quartz.couchbase.TriggerState;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.Optional;
import java.util.stream.StreamSupport;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;

@Slf4j
public class DenormalizedCouchbaseDelegate extends CouchbaseDelegate {

    @Builder
    public DenormalizedCouchbaseDelegate(Bucket bucket, String schedulerName) {
        super(bucket, schedulerName);
    }

    public void storeJobWithTrigger(JobDetail job, OperableTrigger trigger) {
        String jobId = job.getKey().toString();
        JsonObject triggerObject = convertTrigger(trigger).put("state", READY.toString());
        JsonDocument document = bucket.get(jobId);

        if (document == null) {
            bucket.insert(JsonDocument.create(jobId, convertJob(job)
                    .put("triggers", JsonArray.from(triggerObject))));
        }
        else {
            document.content().getArray("triggers").add(triggerObject);
            bucket.replace(document);
        }
    }

    public ListMultimap<JobKey, TriggerKey> selectTriggerKeys(TriggerState state, long maxNextFireTime, int maxCount)
            throws JobPersistenceException {
        N1qlQueryResult result = query(select(
                ipath("job", "name").as("jobName"),
                ipath("job", "group").as("jobGroup"),
                ipath("triggers", "name").as("triggerName"),
                ipath("triggers", "group").as("triggerGroup"))
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
                        (map, row) -> map.put(jobKey(row.getString("jobName"), row.getString("jobGroup")),
                                triggerKey(row.getString("triggerName"), row.getString("triggerGroup"))),
                        ArrayListMultimap::putAll);
    }

    public Optional<OperableTrigger> updateTriggerState(
            JobKey jobKey, TriggerKey triggerKey, TriggerState from, TriggerState to)
            throws JobPersistenceException {
        JsonDocument document = bucket.get(jobKey.toString());
        Optional<OperableTrigger> trigger = find(document.content(), triggerKey)
                .filter(t -> TriggerState.valueOf(t.getString("state")) == from)
                .map(t -> t.put("state", to.toString()))
                .map(this::convertTrigger);

        trigger.ifPresent(x -> bucket.replace(document));
        return trigger;
    }

    private Optional<JsonObject> find(JsonObject content, TriggerKey triggerKey) {
        return StreamSupport.stream(content.getArray("triggers").spliterator(), false)
                .peek(t -> log.info("Got: {}", t))
                .map(JsonObject.class::cast)
                .filter(t -> triggerKey(t.getString("name"), t.getString("group")).equals(triggerKey))
                .peek(t -> log.info("Found: {}", t))
                .findAny();
    }

    @Override
    protected JsonObject convertTrigger(OperableTrigger trigger) {
        return super.convertTrigger(trigger)
                .removeKey("jobName")
                .removeKey("jobGroup");
    }

    @Value
    public static class JobTriggerKey {
        private final JobKey jobKey;
        private final TriggerKey triggerKey;
        private final int triggerIndex;
    }

    public static void main(String[] args) {

        Statement statement = select(
                ipath("triggers", "name"),
                ipath("triggers", "group"))
                .from("quartz job")
                .unnest("triggers");
        System.out.println(statement);
    }
}
