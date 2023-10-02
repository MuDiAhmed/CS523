package org.cs523;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class Producer
{
    public static void main( String[] args )
    {
        Config applicationConf = ConfigFactory.parseResources("application.conf").resolve();
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationConf.getString("spark.kafka.bootstrap.servers"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, applicationConf.getString("spark.kafka.key.serializer"));
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, applicationConf.getString("spark.kafka.value.serializer"));

        String csvFilePath = applicationConf.getString("spark.data.sample.path");

        try (KafkaProducer<Long, String> producer = new KafkaProducer<>(properties); Reader reader = new FileReader(csvFilePath)) {

            String kafkaTopic = applicationConf.getString("spark.kafka.topics.in");
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
            for (CSVRecord record : records) {
                String csvData = String.join(",", record.values());
                ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(kafkaTopic, csvData);
                producer.send(producerRecord);
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
