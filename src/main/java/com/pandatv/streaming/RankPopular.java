package com.pandatv.streaming;

import com.google.common.base.Splitter;
import com.pandatv.bean.RankProject;
import org.apache.commons.lang3.StringUtils;
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
 * @create: 2018-11-21 11:28
 **/
public class RankPopular {
    private static final Logger logger = LogManager.getLogger(RankPopular.class);

    //测试环境
    private static String topics = "pcgameq_shadow_show_person_num";
    private static String groupId = "popular_rank_stream_test";
    private static String bootServers = "10.131.7.20:9092,10.131.7.31:9092,10.131.7.25:9092";//测试环境消费礼物地址(t10-12v.infra.bjtb.pdtv.it) KM:http://t12v.infra.bjtb.pdtv.it:9090/clusters/beta_bjtb
    private static String redisHost = "10.131.7.48";
    private static String redisPwd = "";
    private static int redisPort = 6099;

    private static String projectKey = "rank:gift:projectMap";


    //test ckafka
//    private static String topics = "pcgameq_shadow_show_person_num";
//    private static String groupId = "popular_rank_stream_test";
//    private static String bootServers = "10.131.6.79:9092";
//    private static String redisHost = "localhost";
//    private static String redisPwd = "";
//    private static int redisPort = 6379;

    //线上
//    private static String topics = "pcgameq_shadow_show_person_num";
//    private static String bootServers = "10.131.10.27:9092";//kafkabiz6-10v.infra.bjtb.pdtv.it，worker服务器需要配置hosts
//    private static String groupId = "popular_rank_stream";
//    private static String redisHost = "10.131.11.151";
//    private static String redisPwd = "Hdx03DqyIwOSrEDU";
//    private static int redisPort = 6974;

    public static void main(String[] args) {
        if (args.length == 1) {
            topics = args[0].trim();
        }
        if (args.length == 2) {
            Map<String, String> map = Splitter.on(",").withKeyValueSeparator("=").split(args[1]);
            initParams(map);
        }
        SparkConf conf = new SparkConf().setAppName("popular_rank");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "100");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaSparkContext context = ssc.sparkContext();

        Broadcast<String> redisHostBroadcast = context.broadcast(redisHost);
        Broadcast<Integer> redisPortBroadcast = context.broadcast(redisPort);
        Broadcast<String> redisPwdBroadcast = context.broadcast(redisPwd);


        JavaInputDStream<ConsumerRecord<String, String>> message = initMessage(ssc, args);

        message.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            Map<String, RankProject> rankProjectMap = getProjectMap();
            rdd.map(r -> r.value()).foreachPartition(p -> {

            });
        });

    }

    private static Map<String, RankProject> getProjectMap() {
        Jedis jedis = new Jedis(redisHost, redisPort);
        if (StringUtils.isNotEmpty(redisPwd)) {
            jedis.auth(redisPwd);
        }
        Map<String, RankProject> projectsMap = new HashMap<>();
        Map<String, String> projectMap = jedis.hgetAll(projectKey);
        for (Map.Entry<String, String> entry : projectMap.entrySet()) {
            try {
                String key = entry.getKey();
                String value = entry.getValue();
                Map<String, String> paramMap = Splitter.on(",").withKeyValueSeparator("=").split(value);
                if (!paramMap.containsKey("project") || !paramMap.containsKey("startTimeU") || !paramMap.containsKey("endTimeU") || !paramMap.containsKey("flag") || !paramMap.containsKey("popularRank")) {
                    logger.error("参数配置出错，key:" + key + ";value:" + value);
                    continue;
                }
                RankProject rankProject = new RankProject();
                rankProject.setProject(key);
                String cates = paramMap.getOrDefault("cates", "");
                if (StringUtils.isNotEmpty(cates)) {
                    List<String> cateList = Arrays.asList(cates.split("-"));
                    rankProject.setCates(cateList);
                }
                rankProject.setStartTimeU(Long.parseLong(paramMap.get("startTimeU").substring(0, 10)));
                rankProject.setEndTimeU(Long.parseLong(paramMap.get("endTimeU").substring(0, 10)));
                rankProject.setPopularRank(Boolean.parseBoolean(paramMap.get("popularRank")));
                int flag = Integer.parseInt(paramMap.get("flag"));
                rankProject.setFlag(flag);
                projectsMap.put(key, rankProject);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        jedis.close();
        return projectsMap;
    }

    private static JavaInputDStream<ConsumerRecord<String, String>> initMessage(JavaStreamingContext ssc, String[] args) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "earliest");
//        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        List<String> topicList = Arrays.asList(topics.split(","));
        JavaInputDStream<ConsumerRecord<String, String>> message = null;
        return KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicList, kafkaParams));
    }

    private static void initParams(Map<String, String> map) {
        if (map.containsKey("groupId")) {
            groupId = map.get("groupId");
        }
        //暂时不通过配置方式，因为包含逗号
//        if (map.containsKey("bootServers")) {
//            bootServers = map.get("bootServers");
//        }
        if (map.containsKey("redisHost")) {
            redisHost = map.get("redisHost");
        }
        if (map.containsKey("redisPwd")) {
            redisPwd = map.get("redisPwd");
        }
        if (map.containsKey("redisPort")) {
            redisPort = Integer.parseInt(map.get("redisPort"));
        }
        logger.info("groupId:" + groupId);
    }
}
