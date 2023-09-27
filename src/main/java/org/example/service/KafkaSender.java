package org.example.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Map;

public class KafkaSender implements java.io.Serializable {
    private final KafkaProducer<String, String> producer;

    private KafkaSender(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public void send(String topic, String value) {
        producer.send(new ProducerRecord<>(topic, value));
    }

    public static KafkaSender apply(Map<String, Object> config) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        Thread shutdownHook = new Thread(producer::close);
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        return new KafkaSender(producer);
    }
}