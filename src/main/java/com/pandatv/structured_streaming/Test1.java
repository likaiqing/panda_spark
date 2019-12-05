package com.pandatv.structured_streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


import java.util.Arrays;

/**
 * @author: likaiqing
 * @create: 2019-03-26 21:59
 **/
public class Test1 {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("test")
                .master("local[2]")
                .getOrCreate();
        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();
        Dataset<String> words = lines.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>) str -> Arrays.asList(str.split(" ")).iterator(), Encoders.STRING());

//        words.show();
        words.printSchema();
        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
