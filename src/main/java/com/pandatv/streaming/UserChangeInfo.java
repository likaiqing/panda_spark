package com.pandatv.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
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
import org.apache.spark.streaming.kafka010.*;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: likaiqing
 * @create: 2018-11-14 11:09
 **/
public class UserChangeInfo {
    private static final Logger logger = LogManager.getLogger(UserChangeInfo.class);

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("user_change_info");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "100");
        /**
         * //TODO 使用checkpoint
         */
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaInputDStream<ConsumerRecord<String, String>> message = initChangeMessage(ssc, args);
        JavaSparkContext context = ssc.sparkContext();


        String redisHost = "10.131.11.151";
        String redisPwd = "Hdx03DqyIwOSrEDU";
        int redisPort = 6974;

        Broadcast<String> redisHostBroadcast = context.broadcast(redisHost);
        Broadcast<Integer> redisPortBroadcast = context.broadcast(redisPort);
        Broadcast<String> redisPwdBroadcast = context.broadcast(redisPwd);
        /**
         * 修改昵称，修改房间号实时数据
         */
        message.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            try {
                rdd.map(r -> r.value()).foreachPartition(p -> {
                    ObjectMapper mapper = null;
                    Jedis jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                    if (StringUtils.isNotEmpty(redisPwdBroadcast.value())) {
                        jedis.auth(redisPwdBroadcast.value());
                    }
                    while (p.hasNext()) {
                        if (null == mapper) {
                            mapper = new ObjectMapper();
                        }
                        String next = p.next();
                        String data = mapper.readTree(next).get("data").asText();
                        JsonNode dataNode = mapper.readTree(data);
                        if (dataNode.has("rid")) {//修改昵称头像
                            String rid = dataNode.get("rid").asText();
                            String anchorKey = "panda:zhaomu:ancdtl:" + rid;
                            String userKey = "panda:zhaomu:usrdtl:" + rid;
                            if (jedis.exists(anchorKey)) {
                                String anchorDetail = jedis.get(anchorKey);
                                HashMap map = mapper.readValue(anchorDetail, HashMap.class);
                                if (dataNode.has("nickname")) {
                                    map.put("nickName", dataNode.get("nickname").asText());
                                }
                                if (dataNode.has("avatar")) {
                                    map.put("avatar", dataNode.get("avatar").asText());
                                }
                                jedis.set(anchorKey, mapper.writeValueAsString(map));
                            }
                            if (jedis.exists(userKey)) {
                                String userDetail = jedis.get(userKey);
                                HashMap map = mapper.readValue(userDetail, HashMap.class);
                                if (dataNode.has("nickname")) {
                                    map.put("nickName", dataNode.get("nickname").asText());
                                }
                                if (dataNode.has("avatar")) {
                                    map.put("avatar", dataNode.get("avatar").asText());
                                }
                                jedis.set(anchorKey, mapper.writeValueAsString(map));
                                logger.warn("edit anchorKey:" + anchorKey + ";result:" + mapper.writeValueAsString(map));
                            }
                        } else if (dataNode.has("hostid")) {//修改房间号
                            String rid = dataNode.get("hostid").asText();
                            String anchorKey = "panda:zhaomu:ancdtl:" + rid;
                            if (jedis.exists(anchorKey)) {
                                String anchorDetail = jedis.get(anchorKey);
                                HashMap map = mapper.readValue(anchorDetail, HashMap.class);
                                map.put("roomId", dataNode.get("new_id").asText());
                                jedis.set(anchorKey, mapper.writeValueAsString(map));
                                logger.warn("edit anchorKey:" + anchorKey + ";result:" + mapper.writeValueAsString(map));
                            }
                        }

                    }
                    if (null != jedis) {
                        jedis.close();
                    }
                });

            } catch (Exception e) {
                e.printStackTrace();
            }
            ((CanCommitOffsets) message.inputDStream()).commitAsync(offsetRanges);//处理出问题，说明数据有问题，过滤
        });
        ssc.start();
        ssc.awaitTermination();

    }

    /**
     * pcgameq_villa_change_roomid修改房间号
     * pcgameq_ruc_profile_change修改昵称和头像
     * KafkaManager  http://kafkabiz3v.infra.bjtb.pdtv.it:9090
     *
     * @param ssc
     * @param args
     * @return
     */
    private static JavaInputDStream<ConsumerRecord<String, String>> initChangeMessage(JavaStreamingContext ssc, String[] args) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.131.12.126:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "zhaomu_user_change_info");
        kafkaParams.put("auto.offset.reset", "earliest");
//        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        List<String> topicList = Arrays.asList("pcgameq_villa_change_roomid,pcgameq_ruc_profile_change".split(","));
        JavaInputDStream<ConsumerRecord<String, String>> message = null;
        return KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicList, kafkaParams));
    }
}
