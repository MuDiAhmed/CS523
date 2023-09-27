package org.example.service;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;

public interface Processor<T, R> extends java.io.Serializable {
    JavaDStream<R> process(JavaInputDStream<T> stream);
}
