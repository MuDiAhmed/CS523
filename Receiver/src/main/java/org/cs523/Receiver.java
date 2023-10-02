package org.cs523;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;
import java.util.Arrays;

public class Receiver {
    public static void main(String[] args) throws StreamingQueryException, IOException {

        Config applicationConf = ConfigFactory.parseResources("application.conf").resolve();
        String applicationName = applicationConf.getString("spark.app.name");
        String applicationMaster = applicationConf.getString("spark.app.master");
        String kafkaBootstrapServer = applicationConf.getString("spark.kafka.bootstrap.servers");
        String kafkaTopic = applicationConf.getString("spark.kafka.topics.in");
//        HBase.initiate();

        SparkSession spark = SparkSession
                .builder()
                .appName(applicationName)
                .config("spark.master", applicationMaster)
                .config("spark.streaming.backpressure.enabled", true)
                .config("spark.streaming.backpressure.initialRate", 10)
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServer)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "earliest")
                .option("spark.streaming.backpressure.enabled", true)
                .option("spark.streaming.backpressure.initialRate", 10)
                .load();
//                .selectExpr("CAST(value AS STRING)");


        //TODO:: decide business data schema based on producer
//        Dataset<Row> wordCounts = lines
//                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING())
//                .groupBy("value")
//                .count();


        //TODO:: insert data using HBase insert
        StreamingQuery query = lines
                .writeStream()
                .format("console")
                .outputMode("append")
                .start();

        query.awaitTermination();
    }
}
