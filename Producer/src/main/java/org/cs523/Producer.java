package org.cs523;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer
{
    public static void main( String[] args )
    {
        Config applicationConf = ConfigFactory.parseResources("application.conf").resolve();
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, applicationConf.getString("spark.kafka.bootstrap.servers"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, applicationConf.getString("spark.kafka.key.serializer"));
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, applicationConf.getString("spark.kafka.value.serializer"));

        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<Long, String> producerRecord =
                new ProducerRecord<>(applicationConf.getString("spark.kafka.topics.in"), "hello world");
        producer.send(producerRecord);
        producer.flush();
        producer.close();


    }
}
