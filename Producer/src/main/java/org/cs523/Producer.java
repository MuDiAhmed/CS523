package org.cs523;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
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

        String csvFilePath = "src/main/data/NYC_jobs.csv";
        String kafkaTopic = applicationConf.getString("spark.kafka.topics.in");

        try (Reader reader = new FileReader(csvFilePath)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
            for (CSVRecord record : records) {
                String csvData = record.toString();
                ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(kafkaTopic, csvData);
                producer.send(producerRecord);
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}
