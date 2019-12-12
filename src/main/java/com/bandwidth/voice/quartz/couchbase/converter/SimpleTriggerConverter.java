package com.bandwidth.voice.quartz.couchbase.converter;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

import com.couchbase.client.java.document.json.JsonObject;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.triggers.SimpleTriggerImpl;

public class SimpleTriggerConverter extends TriggerConverter<SimpleTriggerImpl> {

    public SimpleTriggerConverter() {
        super(SimpleTriggerImpl.class, "SIMPLE");
    }

    @Override
    public JsonObject convert(SimpleTriggerImpl trigger, JsonObject object) {
        return object
                .put("repeatCount", trigger.getRepeatCount())
                .put("repeatInterval", trigger.getRepeatInterval())
                .put("timesTriggered", trigger.getTimesTriggered());
    }

    @Override
    public SimpleTriggerImpl convert(JsonObject object, TriggerBuilder<Trigger> builder) {
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) builder
                .withSchedule(simpleSchedule()
                        .withRepeatCount(object.getInt("repeatCount"))
                        .withIntervalInMilliseconds(object.getLong("repeatInterval")))
                .build();
        trigger.setTimesTriggered(object.getInt("timesTriggered"));
        return trigger;
    }
}
