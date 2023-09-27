package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.example.model.Item;
import org.example.service.*;
import java.util.*;

public class App 
{
    public static void main( String[] args ) throws InterruptedException {
        Config applicationConf = ConfigFactory.parseResources("org/example/application.conf").resolve();
        String master = applicationConf.getString("spark.app.master");
        String applicationName = applicationConf.getString("spark.app.name");

        SparkConf conf = new SparkConf().setMaster(master).setAppName(applicationName);

        String checkpointDirectory = applicationConf.getString("spark.kafka.checkpoint.directory");

        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, () -> {
            long durationType = applicationConf.getLong("spark.kafka.batch.duration.duration");
            Duration duration;

            switch (applicationConf.getString("spark.kafka.batch.duration.type").toLowerCase()) {
                case "seconds":
                    duration = Durations.seconds(durationType);
                    break;
                case "milliseconds":
                    duration = Durations.milliseconds(durationType);
                    break;
                case "minutes":
                    duration = Durations.minutes(durationType);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid duration type");
            }

            JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, duration);
            javaStreamingContext.checkpoint(checkpointDirectory);

            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", applicationConf.getString("spark.kafka.bootstrap.servers"));
            kafkaParams.put("auto.offset.reset", applicationConf.getString("spark.kafka.auto.offset.reset"));
            kafkaParams.put("key.serializer", applicationConf.getString("spark.kafka.key.serializer"));
            kafkaParams.put("value.serializer", applicationConf.getString("spark.kafka.value.serializer"));
            kafkaParams.put("key.deserializer", applicationConf.getString("spark.kafka.key.deserializer"));
            kafkaParams.put("value.deserializer", applicationConf.getString("spark.kafka.value.deserializer"));

            List<String> inTopicsList = applicationConf.getStringList("spark.kafka.topics.in");
            Set<String> inTopics = new HashSet<>(inTopicsList);

            ConsumerStrategy<String, String> consumerStrategy = ConsumerStrategies.Subscribe(inTopics, kafkaParams);
            JavaInputDStream<ConsumerRecord<String, String>> kstream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(), consumerStrategy);

            Map<String, Object> kafkaSenderParams = new HashMap<>();
            kafkaSenderParams.put("bootstrap.servers", applicationConf.getString("spark.kafka.bootstrap.servers"));
            kafkaSenderParams.put("key.serializer", applicationConf.getString("spark.kafka.key.serializer"));
            kafkaSenderParams.put("value.serializer", applicationConf.getString("spark.kafka.value.serializer"));
            kafkaSenderParams.put("key.deserializer", applicationConf.getString("spark.kafka.key.deserializer"));
            kafkaSenderParams.put("value.deserializer", applicationConf.getString("spark.kafka.value.deserializer"));

            KafkaSender kafkaSender = KafkaSender.apply(kafkaSenderParams);
            Broadcast<KafkaSender> broadcast = javaStreamingContext.sparkContext().broadcast(kafkaSender);

            List<String> outTopicsList = applicationConf.getStringList("spark.kafka.topics.out");
            Set<String> outTopics = new HashSet<>(outTopicsList);

            Action<Item, KafkaSender> itemAction = new ActionImpl();
            Processor<ConsumerRecord<String, String>, Item> itemProcessor = new ProcessorImpl();
            itemAction.perform(itemProcessor.process(kstream), outTopics, broadcast);

            return javaStreamingContext;
        });

        ssc.start();
        ssc.awaitTermination();

    }
}
