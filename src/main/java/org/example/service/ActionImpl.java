package org.example.service;


import lombok.extern.slf4j.Slf4j;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaRDD;
import org.example.model.Item;
import play.libs.Json;

import java.util.Set;

@Slf4j
public class ActionImpl implements Action<Item, KafkaSender> {
    @Override
    public void performRdd(JavaRDD<Item> rdd, Set<String> topics, Broadcast<KafkaSender> broadcast) {
        rdd.foreach((VoidFunction<Item>) item -> {
            for (String topic : topics) {
                log.info("Sending item to Kafka topic, " + topic + ": " + item);
                broadcast.value().send(topic, Json.toJson(item).toString());
                log.info("Sent item to Kafka topic, " + topic + ": " + item);
            }
        });
    }
}
