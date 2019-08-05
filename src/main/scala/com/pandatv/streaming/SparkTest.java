package com.pandatv.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author: likaiqing
 * @create: 2019-03-14 22:21
 **/
public class SparkTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3);
        JavaRDD<Integer> rdd = sc.parallelize(data, 3);
//        rdd.flatMap()
        rdd.mapPartitions(a -> {
            System.out.println(a);
            return a;
        });

        rdd.foreachPartition(p -> {
            System.out.println("par");
        });
        sc.stop();
    }
}
