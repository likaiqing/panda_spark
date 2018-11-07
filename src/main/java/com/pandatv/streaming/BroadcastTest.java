package com.pandatv.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: likaiqing
 * @create: 2018-11-01 21:04
 **/
public class BroadcastTest {

    private static String bootServers = "10.131.7.20:9092,10.131.7.31:9092,10.131.7.25:9092";
    private static String groupId = "gift_rank_stream_test";
    private static String topics = "pcgameq_panda_gift_donate";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("rank_gift");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "30");
        /**
         * //TODO 使用checkpoint
         */
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaSparkContext context = ssc.sparkContext();
        Broadcast<String> broadcast = context.broadcast("123");
        JavaInputDStream<ConsumerRecord<String, String>> message = initMessage(ssc, args);
//        message.foreachRDD(rdd -> {
//            System.out.println("broadcast:" + broadcast.value());
//            broadcast.unpersist(true);
//            broadcast = context.broadcast("456" + Math.random());
//            rdd.map(r -> r.value()).foreachPartition(p -> {
//                while (p.hasNext()) {
//                    String next = p.next();
//                    System.out.println(next);
//                }
//            });
//        });
    }

    private static JavaInputDStream<ConsumerRecord<String, String>> initMessage(JavaStreamingContext ssc, String[] args) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "earliest");
//        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        List<String> topicList = Arrays.asList(topics.split(","));
        JavaInputDStream<ConsumerRecord<String, String>> message = null;
        return KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicList, kafkaParams));
    }

}
