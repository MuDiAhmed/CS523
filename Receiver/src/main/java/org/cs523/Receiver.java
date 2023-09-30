package org.cs523;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;
import java.util.Arrays;

public class Receiver {
    public static void main(String[] args) throws StreamingQueryException, IOException {

        Config applicationConf = ConfigFactory.parseResources("application.conf").resolve();
        String applicationName = applicationConf.getString("spark.app.name");
        String kafkaBootstrapServer = applicationConf.getString("spark.kafka.bootstrap.servers");
        String topics = applicationConf.getStringList("spark.kafka.topics.in").stream().reduce("", (topicsString, value) -> topicsString +", "+ value);
        HBase.initiate();

        SparkSession spark = SparkSession
                .builder()
                .appName(applicationName)
                .getOrCreate();

        Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServer)
                .option("subscribe", topics)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        //TODO:: decide business data schema based on producer
        Dataset<Row> wordCounts = lines.flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING()).groupBy("value").count();


        //TODO:: insert data using HBase insert
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
