package com.bandwidth.voice.quartz.couchbase.store;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDetail;
import org.quartz.spi.OperableTrigger;

@Slf4j
public class DenormalizedCouchbaseDelegate extends CouchbaseDelegate {

    @Builder
    public DenormalizedCouchbaseDelegate(Bucket bucket, String schedulerName) {
        super(bucket, schedulerName);
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

    @Override
    protected JsonObject convertTrigger(OperableTrigger trigger) {
        return super.convertTrigger(trigger)
                .removeKey("jobName")
                .removeKey("jobGroup");
    }
}
