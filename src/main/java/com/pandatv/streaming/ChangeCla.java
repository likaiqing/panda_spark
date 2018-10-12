package com.pandatv.streaming;

import com.pandatv.tools.RedisClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
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

    //    private static RedisClient redisClient;//如果有内部类，使用到此变量使用全局变量(定时更新某些广播变量的线程)
    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.131.6.79:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "streaming_changecate_test");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);
        SparkConf conf = new SparkConf().setAppName("panda_classify_stream");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaSparkContext sparkContext = ssc.sparkContext();

        JavaInputDStream<ConsumerRecord<Object, Object>> message = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Arrays.asList("panda_realtime_panda_classify_stream"), kafkaParams));

        message.map(m -> m.value()).foreachRDD(rdd -> {
            rdd.foreachPartition(par -> {
                Jedis jedis = new Jedis("10.131.10.12", 6708);
                jedis.auth("A8VDrZfNnkEMZtnp");
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
                    pipelined.zadd(new StringBuffer("changeCla:").append(roomId).toString(), timeU, newClaEname);
                }
                pipelined.sync();
                pipelined.close();
                jedis.close();
            });
        });
        ssc.start();
        ssc.awaitTermination();
    }
}
