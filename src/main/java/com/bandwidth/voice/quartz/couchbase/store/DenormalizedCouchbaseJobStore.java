package com.bandwidth.voice.quartz.couchbase.store;

import static com.bandwidth.voice.quartz.couchbase.TriggerState.ACQUIRED;
import static com.bandwidth.voice.quartz.couchbase.TriggerState.READY;

import com.bandwidth.voice.quartz.couchbase.store.DenormalizedCouchbaseDelegate.JobTriggerKey;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.google.common.collect.ListMultimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;

@Slf4j
@NoArgsConstructor
public class DenormalizedCouchbaseJobStore extends CouchbaseJobStore {

    private DenormalizedCouchbaseDelegate couchbase;

    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) {
        log.info("Initializing {} with bucket '{}'", getClass().getSimpleName(), bucketName);
        CouchbaseCluster cluster = CouchbaseCluster.create(getEnvironment(), clusterNodes)
                .authenticate(clusterUsername, clusterPassword);
        Bucket bucket = Optional.ofNullable(bucketPassword)
                .map(password -> cluster.openBucket(bucketName, password))
                .orElseGet(() -> cluster.openBucket(bucketName));
        couchbase = DenormalizedCouchbaseDelegate.builder()
                .schedulerName(instanceName)
                .bucket(bucket)
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
        couchbase.storeJobWithTrigger(job, trigger);
    }

    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws JobPersistenceException {
        log.trace("storeJobsAndTriggers: {}, {}", triggersAndJobs, replace);
        for (var triggerAndJob : triggersAndJobs.entrySet()) {
            for (Trigger trigger : triggerAndJob.getValue()) {
                couchbase.storeJobWithTrigger(triggerAndJob.getKey(), (OperableTrigger) trigger);
            }
        }
    }

    public void storeJob(JobDetail job, boolean replaceExisting) throws JobPersistenceException {
        log.trace("storeJob: {}, {}", job, replaceExisting);
        throw new UnsupportedOperationException("storeJob");
    }

    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        log.trace("removeJob: {}", jobKey);
        throw new UnsupportedOperationException("removeJob");
    }

    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        log.trace("removeJobs: {}", jobKeys);
        throw new UnsupportedOperationException("removeJobs");
    }

    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        log.trace("retrieveJob: {}", jobKey);
        throw new UnsupportedOperationException("retrieveJob");
    }

    public void storeTrigger(OperableTrigger trigger, boolean replaceExisting) throws JobPersistenceException {
        log.trace("storeTrigger: {}, {}", trigger, replaceExisting);
        throw new UnsupportedOperationException("storeTrigger");
    }

    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.trace("removeTrigger: {}", triggerKey);
        throw new UnsupportedOperationException("removeTrigger");
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        log.trace("removeTriggers: {}", triggerKeys);
        throw new UnsupportedOperationException("removeTriggers");
    }

    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        log.trace("replaceTrigger: {}, {}", triggerKey, newTrigger);
        throw new UnsupportedOperationException("replaceTrigger");
    }

    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        log.trace("retrieveTrigger: {}", triggerKey);
        throw new UnsupportedOperationException("retrieveTrigger");
    }

    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        log.trace("checkExists: {}", jobKey);
        throw new UnsupportedOperationException("checkExists");
    }

    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        log.trace("checkExists: {}", triggerKey);
        throw new UnsupportedOperationException("checkExists");
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
        long maxNextFireTime = Math.max(noLaterThan, System.currentTimeMillis()) + timeWindow;

        ListMultimap<JobKey, TriggerKey> jobTriggerKeys = couchbase.selectTriggerKeys(READY, maxNextFireTime, maxCount);
        log.debug("Trying to acquire {} triggers", jobTriggerKeys.size());

        for (var jobTriggerKey : jobTriggerKeys.entries()) {
            log.trace("Trying to acquire trigger: {}", jobTriggerKey.getValue());
            couchbase.updateTriggerState(jobTriggerKey.getKey(), jobTriggerKey.getValue(), READY, ACQUIRED)
                    .ifPresent(trigger -> {
                        log.trace("Acquired trigger: {}", trigger.getKey());
                        triggers.add(trigger);
                    });
        }
        return null;
    }

    public void releaseAcquiredTrigger(OperableTrigger trigger) {
        throw new UnsupportedOperationException("releaseAcquiredTrigger");
    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        log.trace("triggersFired: {}", triggers);
        return null;
    }

    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
            CompletedExecutionInstruction instruction) {
        log.trace("triggeredJobComplete: {}, {}, {}", trigger, jobDetail, instruction);
    }

    public long getAcquireRetryDelay(int failureCount) {
        throw new UnsupportedOperationException("getAcquireRetryDelay");
    }
}
