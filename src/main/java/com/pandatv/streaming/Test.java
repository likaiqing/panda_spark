package com.pandatv.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: likaiqing
 * @create: 2018-10-22 20:36
 **/
public class Test {
    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.131.6.79:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "gift_rank_stream_test");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        SparkConf conf = new SparkConf().setAppName("test");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "30");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
//        JavaInputDStream<ConsumerRecord<String, String>> message = KafkaUtils.createDirectStream(
//                ssc,
//                LocationStrategies.PreferConsistent(),
//                ConsumerStrategies.<String, String>Subscribe(Arrays.asList("panda_present_detail_test".split(",")), kafkaParams));
        ArrayList<TopicPartition> list = new ArrayList<>();
        list.add((new TopicPartition("panda_present_detail_test", 0)));
        JavaInputDStream<ConsumerRecord<String, String>> message = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Assign(list, kafkaParams));//新加分区，如果没有设置，将会消费不到


        message.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.map(r -> r.value()).foreachPartition(p -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                if (o.fromOffset() != o.untilOffset()) {
                    System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset() + ";");
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
