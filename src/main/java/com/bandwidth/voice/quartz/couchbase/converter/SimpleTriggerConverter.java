package com.bandwidth.voice.quartz.couchbase.converter;

import com.couchbase.client.java.document.json.JsonObject;
import org.quartz.SimpleTrigger;

public class SimpleTriggerConverter extends TriggerConverter<SimpleTrigger> {

    public SimpleTriggerConverter() {
        super(SimpleTrigger.class, "SIMPLE");
    }

    @Override
    public JsonObject convert(SimpleTrigger trigger) {
        return super.convert(trigger)
                .put("repeatCount", trigger.getRepeatCount())
                .put("repeatInterval", trigger.getRepeatInterval())
                .put("timesTriggered", trigger.getTimesTriggered());
    }
}
