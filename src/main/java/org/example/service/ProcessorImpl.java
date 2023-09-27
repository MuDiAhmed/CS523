package org.example.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.example.model.Item;

import java.io.IOException;

public class ProcessorImpl implements Processor<ConsumerRecord<String, String>, Item> {
    @Override
    public JavaDStream<Item> process(JavaInputDStream<ConsumerRecord<String, String>> stream) {
        return stream.map(ConsumerRecord::value).map(json -> {
            try {
                ObjectMapper mapper = new ObjectMapper();
                Item item = mapper.readValue(json, Item.class);
//                log.info("~~~Processing: " + item);
                return item;
            } catch (IOException e) {
                e.printStackTrace();
                return null; // Handle the exception or return a default value
            }
        });
    }
}
