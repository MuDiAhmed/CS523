package org.cs523;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class Receiver {
    public static void main(String[] args) throws StreamingQueryException {

        Config applicationConf = ConfigFactory.parseResources("application.conf").resolve();
        String applicationName = applicationConf.getString("spark.app.name");
        String kafkaBootstrapServer = applicationConf.getString("spark.kafka.bootstrap.servers");
        String topics = applicationConf.getStringList("spark.kafka.topics.in").stream().reduce("", (topicsString, value) -> topicsString +", "+ value);

        SparkSession spark = SparkSession
                .builder()
                .appName(applicationName)
                .getOrCreate();

        // Create DataSet representing the stream of input lines from kafka
        Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServer)
                .option("subscribe", topics)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = lines.flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING()).groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
