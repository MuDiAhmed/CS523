package org.cs523;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;

public class Receiver {
    public static void main(String[] args) throws StreamingQueryException, IOException {

        Config applicationConf = ConfigFactory.parseResources("application.conf").resolve();
        String applicationName = applicationConf.getString("spark.app.name");
        String applicationMaster = applicationConf.getString("spark.app.master");
        String kafkaBootstrapServer = applicationConf.getString("spark.kafka.bootstrap.servers");
        String kafkaTopic = applicationConf.getString("spark.kafka.topics.in");
        String kafkaValueDeserializer = applicationConf.getString("spark.kafka.value.deserializer");
        String kafkaKeyDeserializer = applicationConf.getString("spark.kafka.key.deserializer");
//        HBase.initiate();

        SparkSession spark = SparkSession
                .builder()
                .appName(applicationName)
                .config("spark.master", applicationMaster)
                .config("spark.streaming.backpressure.enabled", true)
                .config("spark.streaming.backpressure.initialRate", 10)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ALL");

        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServer)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", false)
                .option("kafkaConsumer.pollTimeoutMs", 30000)
                .option("maxOffsetsPerTrigger", 10)
                .option("key.deserializer", kafkaKeyDeserializer)
                .option("value.deserializer", kafkaValueDeserializer)
                .load()
                .selectExpr("CAST(value AS String)");

        Dataset<String> csvRows = lines
                .flatMap((FlatMapFunction<Row, CSVRecord>) line -> {
                    String value = line.getAs(0);
                    List<CSVRecord> records = CSVParser.parse(value, CSVFormat.DEFAULT).getRecords();
                    return records.iterator();
                }, Encoders.kryo(CSVRecord.class))
                .map((MapFunction<CSVRecord, String>) record -> record.values()[0], Encoders.STRING());

        //TODO:: insert data using HBase insert
        StreamingQuery query = csvRows
                .writeStream()
                .format("console")
                .outputMode("append")
                .start();

        query.awaitTermination();
    }
}
