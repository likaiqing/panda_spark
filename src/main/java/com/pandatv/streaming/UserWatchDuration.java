package com.pandatv.streaming;

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
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import redis.clients.jedis.*;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author: likaiqing
 * @create: 2018-10-09 11:29
 **/
public class UserWatchDuration {
    private static final Logger logger = LogManager.getLogger(UserWatchDuration.class);

    private static final String redisHost = "localhost";
    private static final String redisPwd = "";
    private static final int redisPort = 6379;

//    private static final String redisHost = "10.131.10.12";
//    private static final String redisPwd = "A8VDrZfNnkEMZtnp";
//    private static final int redisPort = 6708;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("kafka_first");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(120));

        JavaSparkContext context = ssc.sparkContext();
        /**
         * 广播redis相关变量
         */
        Broadcast<String> redisHostBroadcast = context.broadcast(redisHost);
        Broadcast<Integer> redisPortBroadcast = context.broadcast(redisPort);
        Broadcast<String> redisPwdBroadcast = context.broadcast(redisPwd);

        /**
         * 广播版区
         */
        List<String> cates = new ArrayList<>();
        cates.add("yzdr");
        Broadcast<List<String>> catesBroadcast = context.broadcast(cates);

        /**
         * 广播流地址与房间号对应关系变量
         */
        Map<String, String> streamRoomIdMap = getStreamRoomIdMap();
        Broadcast<Map<String, String>> streamRoomIdMapBroadcast = context.broadcast(streamRoomIdMap);


        /**
         * 创建dstream(只处理client_online,player_online数据，其他的不接收)
         */
        JavaInputDStream<ConsumerRecord<String, String>> message = initMessage(ssc, args);
        //TODO 考虑切换版区实时任务失败的情况，解决1：当前程序添加消费切换版区数据，同一个消费者组

        message.map(m -> m.value()).filter(l -> !l.contains("uid=0") && !l.contains("uid=-")).mapToPair(l -> {
            try {
                String flag = "p";//player_online
                if (l.contains("client_punch.gif")) {
                    flag = "c";
                }
                int timeIndex = l.indexOf("[");
                String timeStr = l.substring(timeIndex + 1, l.indexOf("]", timeIndex));
                long timeU = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH).parse(timeStr).getTime() / 1000;
                int uidIndex = l.indexOf("uid=");
                int uidEndIndex1 = l.indexOf("&", uidIndex);
                int uidEndIndex2 = l.indexOf(" ", uidIndex);
                int uidEndIndex = 0;
                if (uidEndIndex1 < 0) {
                    uidEndIndex = uidEndIndex2;
                } else {
                    uidEndIndex = uidEndIndex1;
                }
                String uid = l.substring(uidIndex + 4, uidEndIndex);
                int uIndex = l.indexOf("u=");
                String url = l.substring(uIndex + 2, l.indexOf(".flv", uIndex));
                String urlPart = url.split("live_panda%2F")[1];
                String stream = null;
                if (urlPart.contains("_")) {
                    stream = urlPart.split("_")[0];
                } else {
                    stream = urlPart;
                }
                return new Tuple2<String, String>(stream, uid + "-" + timeU + flag);
            } catch (Exception e) {
                e.printStackTrace();
                return new Tuple2<String, String>(null, "-");
            }
        }).filter(f -> {
            return f._1 != null;
        }).reduceByKey((a, b) ->/*按stream将{uid}-{timeU}{flag}变成多个按逗号分隔*/ new StringBuffer(a).append(",").append(b).toString()).mapPartitionsToPair(kv -> {
            /**
             * stream转换成roomId
             */
            Set<Tuple2<String, String>> res = new HashSet<>();
            Jedis jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
            String redisPwd = redisPwdBroadcast.value();
            if (StringUtils.isNotEmpty(redisPwd)) {
                jedis.auth(redisPwd);
            }
            Map<String, String> streamRoomIdMapValue = streamRoomIdMapBroadcast.value();
            while (kv.hasNext()) {
                Tuple2<String, String> next = kv.next();
                String stream = next._1;
                if (streamRoomIdMapValue.containsKey(stream)) {
                    res.add(new Tuple2<String, String>(streamRoomIdMapValue.get(stream), next._2));
                } else {
                    String roomId = jedis.get(new StringBuffer("stream:").append(stream).toString());
                    if (StringUtils.isNotEmpty(roomId)) {
                        res.add(new Tuple2<String, String>(roomId, next._2));
                    }
                }
            }
            jedis.close();
            return res.iterator();
        }).flatMapToPair(kv -> {
            /**
             * 将roomId->{uid}-{timeU}{flag}转成{uid}->{timeU}对
             */
            Set<Tuple2<String, Long>> uidTimeuList = new HashSet<>();
            Jedis jedis = null;
            String redisPwd = redisPwdBroadcast.value();
            try {
                jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                if (StringUtils.isNotEmpty(redisPwd)) {
                    jedis.auth(redisPwd);
                }
                String roomId = kv._1;
                String[] uidTimeuArr = kv._2.split(",");
                Tuple[] tuples = jedis.zrevrangeWithScores(new StringBuffer("room:changecla:").append(roomId).toString(), 0, -1).toArray(new Tuple[]{});
                if (tuples.length == 0) {
                    logger.warn("roomId:" + roomId + ",对应的切换版区数据在redis查询不到");
                } else {
                    List<String> catesValue = catesBroadcast.value();
                    for (int i = 0; i < uidTimeuArr.length; i++) {
                        String uidTime = uidTimeuArr[i];
                        String[] arr = uidTime.split("-");
                        String uid = arr[0];
                        long logTimeU = Long.parseLong(arr[1].substring(0, 10));
                        String cate = null;
                        for (int j = 0; j < tuples.length; j++) {
                            Tuple tuple = tuples[j];
                            double score = tuple.getScore();
                            if (logTimeU < score) {
                                break;
                            }
                            cate = tuple.getElement();
                        }
                        if (StringUtils.isNotEmpty(cate) && catesValue.contains(cate)) {
                            String flag = arr[1].substring(10);
                            long formatLogTimeU = logTimeU / 60 * 60;
                            if (flag.equals("p")) {//pc打点，一分钟一条
                                uidTimeuList.add(new Tuple2<String, Long>(uid, formatLogTimeU));
                            } else {//客户端2分钟一条,加上前一分钟
                                uidTimeuList.add(new Tuple2<String, Long>(uid, formatLogTimeU - 60));
                                uidTimeuList.add(new Tuple2<String, Long>(uid, formatLogTimeU));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (null != jedis) {
                    jedis.close();
                }
            }
            return uidTimeuList.iterator();
        }).groupByKey().foreachRDD(rdd -> {
            Set<String> uids = new HashSet<>();
            rdd.foreachPartition(it -> {
                Jedis jedis = null;
                String redisPwd = redisPwdBroadcast.value();
                try {
                    jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                    if (StringUtils.isNotEmpty(redisPwd)) {
                        jedis.auth(redisPwd);
                    }
                    Pipeline pipelined = jedis.pipelined();
                    while (it.hasNext()) {
                        Tuple2<String, Iterable<Long>> next = it.next();
                        String uid = next._1;
                        Iterator<Long> iterator = next._2.iterator();
                        while (iterator.hasNext()) {
                            pipelined.pfadd(new StringBuffer("pf:").append(uid).toString(), String.valueOf(iterator.next()));
                        }
                        uids.add(uid);
                    }
                    pipelined.sync();
                    pipelined.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (null != jedis) {
                        jedis.close();
                    }
                }
            });
            logger.info("uids.size:" + uids.size());
            Jedis jedis = null;
            try {
                String redisPwd = redisPwdBroadcast.value();
                jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                if (StringUtils.isNotEmpty(redisPwd)) {
                    jedis.auth(redisPwd);
                }
                Map<String, Double> zaddMap = new HashMap<>();
                for (String uid : uids) {
                    long pfcount = jedis.pfcount(new StringBuffer("pf:").append(uid).toString());
                    logger.info("uid:" + uid + ",pfcount:" + pfcount);
                    zaddMap.put(uid, (double) pfcount);
                }
                jedis.zadd("", zaddMap);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (null != jedis) {
                    jedis.close();
                }
            }
        });


        ssc.start();
        ssc.awaitTermination();
    }

    private static JavaInputDStream<ConsumerRecord<String, String>> initMessage(JavaStreamingContext ssc, String[] args) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.131.6.79:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "streaming_test");
        kafkaParams.put("auto.offset.reset", "latest");
        //batch duration大于30秒，需设置以下两个参数
        kafkaParams.put("heartbeat.interval.ms", "130000");//小于session.timeout.ms，最后高于1/3
        kafkaParams.put("session.timeout.ms", "300000");//范围group.min.session.timeout.ms(6000)与group.max.session.timeout.ms(30000)之间
        kafkaParams.put("request.timeout.ms", "310000");
        //batch duration大于5分钟，需在broker设置group.max.session.timeout.ms参数
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList(args[0].split(","));

        KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
        return null;
    }

    /**
     * 只在第一次启动时出示流地址与房间对应关系，不需要定时更新，新房间也不一定会统计，广播变量内没有的话查询reids
     *
     * @return
     */
    private static Map<String, String> getStreamRoomIdMap() {
        Jedis jedis = new Jedis(redisHost, redisPort);
        if (StringUtils.isNotEmpty(redisPwd)) {
            jedis.auth(redisPwd);
        }
        String cursor = "0";
        Map<String, String> map = new HashMap<>();
        ScanParams params = new ScanParams().match("stream:*").count(1000);
        while (!cursor.equals("0")) {
            ScanResult<String> scan = jedis.scan(cursor, params);
            cursor = scan.getStringCursor();
            String key = scan.getResult().get(0);
            map.put(key.substring(key.indexOf(":") + 1), jedis.get(key));
        }
        jedis.close();
        return map;
    }

}
