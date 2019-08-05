package com.pandatv;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: likaiqing
 * @create: 2018-10-29 17:17
 **/
public class OffsetTest {
    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.131.6.79:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "present_client_online_test10");
        kafkaParams.put("auto.offset.reset", "earliest");
        //batch duration大于30秒，需设置以下两个参数
//        kafkaParams.put("heartbeat.interval.ms", "130000");//小于session.timeout.ms，最后高于1/3
//        kafkaParams.put("session.timeout.ms", "300000");//范围group.min.session.timeout.ms(6000)与group.max.session.timeout.ms(30000)之间
//        kafkaParams.put("request.timeout.ms", "310000");
        //batch duration大于5分钟，需在broker设置group.max.session.timeout.ms参数
        kafkaParams.put("enable.auto.commit", true);
        SparkConf conf = new SparkConf().setAppName("present_test");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "5");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaInputDStream<ConsumerRecord<String, String>> message = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(Arrays.asList("panda_realtime_client_online".split(",")), kafkaParams));

        message.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            rdd.foreachPartition(p -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                if (o.fromOffset() != o.untilOffset()) {
                    System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                }
                while (p.hasNext()) {
                    System.out.println(p.next());
                }
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
