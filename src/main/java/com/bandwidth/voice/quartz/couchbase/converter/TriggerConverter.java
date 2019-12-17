package com.bandwidth.voice.quartz.couchbase.converter;

import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.parse;
import static com.bandwidth.voice.quartz.couchbase.CouchbaseUtils.serialize;
import static org.quartz.JobKey.jobKey;

import com.couchbase.client.java.document.json.JsonObject;
import java.util.Objects;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.quartz.JobDataMap;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.spi.OperableTrigger;

@AllArgsConstructor
public abstract class TriggerConverter<T extends OperableTrigger> {

    private final Class<T> triggerType;
    private final String triggerTypeName;

    @SuppressWarnings("unchecked")
    public <U extends OperableTrigger> Optional<TriggerConverter<U>> forType(Class<? extends OperableTrigger> type) {
        return triggerType.isAssignableFrom(type)
                ? Optional.of((TriggerConverter<U>) this)
                : Optional.empty();
    }

    public Optional<TriggerConverter<?>> forType(String type) {
        return Objects.equals(type, triggerTypeName)
                ? Optional.of(this)
                : Optional.empty();
    }

    public final JsonObject convert(T trigger) {
        return convert(trigger, JsonObject.create()
                .put("type", triggerTypeName)
                .put("name", trigger.getKey().getName())
                .put("group", trigger.getKey().getGroup())
                .put("description", trigger.getDescription())
                .put("data", trigger.getJobDataMap())
                .put("jobName", trigger.getJobKey().getName())
                .put("jobGroup", trigger.getJobKey().getGroup())
                .put("startTime", serialize(trigger.getStartTime()))
                .put("endTime", serialize(trigger.getEndTime()))
                .put("nextFireTime", serialize(trigger.getNextFireTime()))
                .put("previousFireTime", serialize(trigger.getPreviousFireTime()))
                .put("priority", trigger.getPriority())
                .put("misfireInstruction", trigger.getMisfireInstruction()));
    }

    public abstract JsonObject convert(T trigger, JsonObject object);

    public final T convert(JsonObject object) {
        T trigger = convert(object, TriggerBuilder.newTrigger()
                .withIdentity(object.getString("name"), object.getString("group"))
                .withDescription(object.getString("description"))
                .usingJobData(parseData(object))
                .forJob(jobKey(object.getString("jobName"), object.getString("jobGroup")))
                .startAt(parse(object.getString("startTime")))
                .endAt(parse(object.getString("endTime")))
                .withPriority(object.getInt("priority")));
        trigger.setNextFireTime(parse(object.getString("nextFireTime")));
        trigger.setPreviousFireTime(parse(object.getString("previousFireTime")));
        trigger.setMisfireInstruction(object.getInt("misfireInstruction"));
        return trigger;
    }

    public abstract T convert(JsonObject object, TriggerBuilder<Trigger> builder);

    private static JobDataMap parseData(JsonObject object) {
        return Optional.ofNullable(object.getObject("data"))
                .map(JsonObject::toMap)
                .map(JobDataMap::new)
                .orElse(null);
    }

    /*
    -- Thanks to Amir Kibbar and Peter Rietzler for contributing the schema for H2 database,
-- and verifying that it works with Quartz's StdJDBCDelegate
--
-- Note, Quartz depends on row-level locking which means you must use the MVCC=TRUE
-- setting on your H2 database, or you will experience dead-locks
--
--
-- In your Quartz properties file, you'll need to set
-- org.quartz.jobStore.driverDelegateClass = org.quartz.impl.jdbcjobstore.StdJDBCDelegate

CREATE TABLE IF NOT EXISTS QRTZ_CALENDARS (
  SCHED_NAME VARCHAR(120) NOT NULL,
  CALENDAR_NAME VARCHAR (200)  NOT NULL ,
  CALENDAR IMAGE NOT NULL
);

CREATE TABLE IF NOT EXISTS QRTZ_CRON_TRIGGERS (
  SCHED_NAME VARCHAR(120) NOT NULL,
  TRIGGER_NAME VARCHAR (200)  NOT NULL ,
  TRIGGER_GROUP VARCHAR (200)  NOT NULL ,
  CRON_EXPRESSION VARCHAR (120)  NOT NULL ,
  TIME_ZONE_ID VARCHAR (80)
);

CREATE TABLE IF NOT EXISTS QRTZ_FIRED_TRIGGERS (
  SCHED_NAME VARCHAR(120) NOT NULL,
  ENTRY_ID VARCHAR (95)  NOT NULL ,
  TRIGGER_NAME VARCHAR (200)  NOT NULL ,
  TRIGGER_GROUP VARCHAR (200)  NOT NULL ,
  INSTANCE_NAME VARCHAR (200)  NOT NULL ,
  FIRED_TIME BIGINT NOT NULL ,
  SCHED_TIME BIGINT NOT NULL ,
  PRIORITY INTEGER NOT NULL ,
  STATE VARCHAR (16)  NOT NULL,
  JOB_NAME VARCHAR (200)  NULL ,
  JOB_GROUP VARCHAR (200)  NULL ,
  IS_NONCONCURRENT BOOLEAN  NULL ,
  REQUESTS_RECOVERY BOOLEAN  NULL
);

CREATE TABLE IF NOT EXISTS QRTZ_PAUSED_TRIGGER_GRPS (
  SCHED_NAME VARCHAR(120) NOT NULL,
  TRIGGER_GROUP VARCHAR (200)  NOT NULL
);

CREATE TABLE IF NOT EXISTS QRTZ_SCHEDULER_STATE (
  SCHED_NAME VARCHAR(120) NOT NULL,
  INSTANCE_NAME VARCHAR (200)  NOT NULL ,
  LAST_CHECKIN_TIME BIGINT NOT NULL ,
  CHECKIN_INTERVAL BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS QRTZ_LOCKS (
  SCHED_NAME VARCHAR(120) NOT NULL,
  LOCK_NAME VARCHAR (40)  NOT NULL
);

CREATE TABLE IF NOT EXISTS QRTZ_JOB_DETAILS (
  SCHED_NAME VARCHAR(120) NOT NULL,
  JOB_NAME VARCHAR (200)  NOT NULL ,
  JOB_GROUP VARCHAR (200)  NOT NULL ,
  DESCRIPTION VARCHAR (250) NULL ,
  JOB_CLASS_NAME VARCHAR (250)  NOT NULL ,
  IS_DURABLE BOOLEAN  NOT NULL ,f
  IS_NONCONCURRENT BOOLEAN  NOT NULL ,
  IS_UPDATE_DATA BOOLEAN  NOT NULL ,
  REQUESTS_RECOVERY BOOLEAN  NOT NULL ,
  JOB_DATA IMAGE NULL
);

CREATE TABLE IF NOT EXISTS QRTZ_SIMPLE_TRIGGERS (
  SCHED_NAME VARCHAR(120) NOT NULL,
  TRIGGER_NAME VARCHAR (200)  NOT NULL ,
  TRIGGER_GROUP VARCHAR (200)  NOT NULL ,
  REPEAT_COUNT BIGINT NOT NULL ,
  REPEAT_INTERVAL BIGINT NOT NULL ,
  TIMES_TRIGGERED BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS qrtz_simprop_triggers
  (
    SCHED_NAME VARCHAR(120) NOT NULL,
    TRIGGER_NAME VARCHAR(200) NOT NULL,
    TRIGGER_GROUP VARCHAR(200) NOT NULL,
    STR_PROP_1 VARCHAR(512) NULL,
    STR_PROP_2 VARCHAR(512) NULL,
    STR_PROP_3 VARCHAR(512) NULL,
    INT_PROP_1 INTEGER NULL,
    INT_PROP_2 INTEGER NULL,
    LONG_PROP_1 BIGINT NULL,
    LONG_PROP_2 BIGINT NULL,
    DEC_PROP_1 NUMERIC(13,4) NULL,
    DEC_PROP_2 NUMERIC(13,4) NULL,
    BOOL_PROP_1 BOOLEAN NULL,
    BOOL_PROP_2 BOOLEAN NULL,
);

CREATE TABLE IF NOT EXISTS QRTZ_BLOB_TRIGGERS (
  SCHED_NAME VARCHAR(120) NOT NULL,
  TRIGGER_NAME VARCHAR (200)  NOT NULL ,
  TRIGGER_GROUP VARCHAR (200)  NOT NULL ,
  BLOB_DATA IMAGE NULL
);

CREATE TABLE IF NOT EXISTS QRTZ_TRIGGERS (
  SCHED_NAME VARCHAR(120) NOT NULL,
  TRIGGER_NAME VARCHAR (200)  NOT NULL ,
  TRIGGER_GROUP VARCHAR (200)  NOT NULL ,
  JOB_NAME VARCHAR (200)  NOT NULL ,
  JOB_GROUP VARCHAR (200)  NOT NULL ,
  DESCRIPTION VARCHAR (250) NULL ,
  NEXT_FIRE_TIME BIGINT NULL ,
  PREV_FIRE_TIME BIGINT NULL ,
  PRIORITY INTEGER NULL ,
  TRIGGER_STATE VARCHAR (16)  NOT NULL ,
  TRIGGER_TYPE VARCHAR (8)  NOT NULL ,
  START_TIME BIGINT NOT NULL ,
  END_TIME BIGINT NULL ,
  CALENDAR_NAME VARCHAR (200)  NULL ,
  MISFIRE_INSTR SMALLINT NULL ,
  JOB_DATA IMAGE NULL
);

ALTER TABLE QRTZ_CALENDARS  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_CALENDARS PRIMARY KEY
  (
    SCHED_NAME,
    CALENDAR_NAME
  );

ALTER TABLE QRTZ_CRON_TRIGGERS  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_CRON_TRIGGERS PRIMARY KEY
  (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  );

ALTER TABLE QRTZ_FIRED_TRIGGERS  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_FIRED_TRIGGERS PRIMARY KEY
  (
    SCHED_NAME,
    ENTRY_ID
  );

ALTER TABLE QRTZ_PAUSED_TRIGGER_GRPS  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_PAUSED_TRIGGER_GRPS PRIMARY KEY
  (
    SCHED_NAME,
    TRIGGER_GROUP
  );

ALTER TABLE QRTZ_SCHEDULER_STATE  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_SCHEDULER_STATE PRIMARY KEY
  (
    SCHED_NAME,
    INSTANCE_NAME
  );

ALTER TABLE QRTZ_LOCKS  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_LOCKS PRIMARY KEY
  (
    SCHED_NAME,
    LOCK_NAME
  );

ALTER TABLE QRTZ_JOB_DETAILS  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_JOB_DETAILS PRIMARY KEY
  (
    SCHED_NAME,
    JOB_NAME,
    JOB_GROUP
  );

ALTER TABLE QRTZ_SIMPLE_TRIGGERS  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_SIMPLE_TRIGGERS PRIMARY KEY
  (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  );

ALTER TABLE QRTZ_SIMPROP_TRIGGERS  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_SIMPROP_TRIGGERS PRIMARY KEY
  (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  );

ALTER TABLE QRTZ_TRIGGERS  ADD
  CONSTRAINT IF NOT EXISTS PK_QRTZ_TRIGGERS PRIMARY KEY
  (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  );

ALTER TABLE QRTZ_CRON_TRIGGERS ADD
  CONSTRAINT IF NOT EXISTS FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY
  (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  ) REFERENCES QRTZ_TRIGGERS (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  ) ON DELETE CASCADE;


ALTER TABLE QRTZ_SIMPLE_TRIGGERS ADD
  CONSTRAINT IF NOT EXISTS FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY
  (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  ) REFERENCES QRTZ_TRIGGERS (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  ) ON DELETE CASCADE;

ALTER TABLE QRTZ_SIMPROP_TRIGGERS ADD
  CONSTRAINT IF NOT EXISTS FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY
  (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  ) REFERENCES QRTZ_TRIGGERS (
    SCHED_NAME,
    TRIGGER_NAME,
    TRIGGER_GROUP
  ) ON DELETE CASCADE;


ALTER TABLE QRTZ_TRIGGERS ADD
  CONSTRAINT IF NOT EXISTS FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS FOREIGN KEY
  (
    SCHED_NAME,
    JOB_NAME,
    JOB_GROUP
  ) REFERENCES QRTZ_JOB_DETAILS (
    SCHED_NAME,
    JOB_NAME,
    JOB_GROUP
  );

COMMIT;

     */
}
