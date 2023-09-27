package org.example.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.Set;

public interface Action<T, R> extends java.io.Serializable {
    default void perform(JavaDStream<T> stream, Set<String> topics, Broadcast<R> broadcast) {
        stream.foreachRDD(new VoidFunction<JavaRDD<T>>() {
            @Override
            public void call(JavaRDD<T> rdd) throws Exception {
                performRdd(rdd, topics, broadcast);
            }
        });
    }

    void performRdd(JavaRDD<T> rdd, Set<String> topics, Broadcast<R> kafkaSender);
}
