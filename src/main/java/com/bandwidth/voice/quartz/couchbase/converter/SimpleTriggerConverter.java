package com.bandwidth.voice.quartz.couchbase.converter;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

import com.couchbase.client.java.document.json.JsonObject;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

public class SimpleTriggerConverter extends TriggerConverter<SimpleTrigger> {

    public SimpleTriggerConverter() {
        super(SimpleTrigger.class, "SIMPLE");
    }

    @Override
    public JsonObject convert(SimpleTrigger trigger, JsonObject object) {
        return object
                .put("repeatCount", trigger.getRepeatCount())
                .put("repeatInterval", trigger.getRepeatInterval())
                .put("timesTriggered", trigger.getTimesTriggered());
    }

    @Override
    public SimpleTrigger convert(JsonObject object, TriggerBuilder<Trigger> trigger) {
        return trigger
                .withSchedule(simpleSchedule()
                .withIntervalInMilliseconds(object.getLong("repeatInterval"))
                .withRepeatCount(object.getInt("repeatCount")))
                .build();
    }

}
