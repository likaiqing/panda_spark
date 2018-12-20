package com.pandatv.streaming;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: likaiqing
 * @create: 2018-10-11 14:58
 * 所有房间切换版区实时数据，按时间戳排序存入zset,已有离线数据没2小时初始化新房价
 **/
public class ChangeCla {
    private static final Logger logger = LogManager.getLogger(ChangeCla.class);

//    private static final String redisHost = "localhost";
//    private static final String redisPwd = "";
//    private static final int redisPort = 6379;

    private static String redisHost = "10.131.11.151";
    private static String redisPwd = "Hdx03DqyIwOSrEDU";
    private static int redisPort = 6974;

//    private static String topic = "panda_realtime_panda_classify_stream";
//    private static String groupId = "streaming_changecate";

    //    private static RedisClient redisClient;//如果有内部类，使用到此变量使用全局变量(定时更新某些广播变量的线程)
    public static void main(String[] args) throws InterruptedException {
        Map<String, String> map = new HashMap<>();
        String topic = "panda_realtime_panda_classify_stream";
        if (args.length > 1) {
            topic = args[0];
            map = Splitter.on(",").withKeyValueSeparator("=").split(args[1]);
        }
        /**
         * 广播redis相关变量
         */
        redisHost = map.getOrDefault("redisHost", "10.131.11.151");
        redisPwd = map.getOrDefault("redisPwd", "Hdx03DqyIwOSrEDU");
        redisPort = Integer.parseInt(map.getOrDefault("redisPort", "6974"));
        String groupId = map.getOrDefault("groupId", "streaming_changecate");
        String appName = map.getOrDefault("name", "panda_classify_stream");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.131.6.79:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaSparkContext context = ssc.sparkContext();
        Broadcast<String> redisHostBroadcast = context.broadcast(redisHost);
        Broadcast<Integer> redisPortBroadcast = context.broadcast(redisPort);
        Broadcast<String> redisPwdBroadcast = context.broadcast(redisPwd);

        String[] topics = map.getOrDefault("topics", topic).split("-");
        JavaInputDStream<ConsumerRecord<Object, Object>> message = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Arrays.asList(topics), kafkaParams));

        message.foreachRDD(rdd -> {
            try {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                rdd.map(r -> r.value()).foreachPartition(par -> {
                    OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//                if (o.fromOffset() != o.untilOffset()) {
//                    System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
//                }
                    Jedis jedis = null;
                    jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                    if (StringUtils.isNotEmpty(redisPwdBroadcast.value())) {
                        jedis.auth(redisPwdBroadcast.value());
                    }
                    Pipeline pipelined = jedis.pipelined();
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    while (par.hasNext()) {
                        String next = par.next().toString();
                        String[] arr = next.split("\t");
                        String roomId = null;
                        String newClaEname = null;
                        long timeU = 0;
                        if (arr.length >= 1) {
                            roomId = arr[0];
                        } else {
                            continue;
                        }
                        if (arr.length >= 4) {
                            newClaEname = arr[3];
                        } else {
                            continue;
                        }
                        if (arr.length >= 6) {
                            try {
                                timeU = format.parse(arr[5]).getTime() / 1000;
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        } else {
                            continue;
                        }
                        pipelined.zadd(new StringBuffer("room:changecla:").append(roomId).toString(), timeU, newClaEname);
                    }
                    pipelined.sync();
                    pipelined.close();
                });
                ((CanCommitOffsets) message.inputDStream()).commitAsync(offsetRanges);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {

            }
        });
        ssc.start();
        ssc.awaitTermination();
    }
}
