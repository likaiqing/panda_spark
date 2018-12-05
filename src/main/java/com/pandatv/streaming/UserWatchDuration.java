package com.pandatv.streaming;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import redis.clients.jedis.Tuple;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author: likaiqing
 * @create: 2018-10-09 11:29
 **/
public class UserWatchDuration {
    private static final Logger logger = LogManager.getLogger(UserWatchDuration.class);

//    private static String redisHost = "localhost";
//    private static String redisPwd = "";
//    private static int redisPort = 6379;

    private static String groupId = "duration_stream_online";
    private static String bootServers = "10.131.6.79:9092";

    //目标kafka
//    10.131.11.17    kafkabiz6v.infra.bjtb.pdtv.it
//    10.131.11.197   kafkabiz7v.infra.bjtb.pdtv.it
//    10.131.10.249   kafkabiz8v.infra.bjtb.pdtv.it
//    10.131.11.65    kafkabiz9v.infra.bjtb.pdtv.it
//    10.131.10.242   kafkabiz10v.infra.bjtb.pdtv.it
    private static String targetBootServers = "10.131.10.27:9092";


    private static String redisHost = "10.131.11.151";
    private static String redisPwd = "Hdx03DqyIwOSrEDU";
    private static int redisPort = 6974;

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            logger.error("Usage:topic1,topic2 project=[project],startTimeU=[startTimeU],endTimeU=[endTimeU],cates=[cate1-cate2],redisHost=[host],redisPort=[port],redisPwd=[pwd],dayMinutes=180,");
            //project=uwdt,startTimeU=1539619888,endTimeU=1540743088,cates=cjzc-pubgm-zjz-sycj-cfmobile-zhcj-kingglory-fcsy-dwrg-mlbb-newgames-fishes-moba-werewolf-shadowverse-kofd-sanguo-cfm-lqsy-ciyuan-naruto-mobilegame-indiegame-club-rxzq-jxsj-girl
            System.exit(1);
        }
        Map<String, String> map = Splitter.on(",").withKeyValueSeparator("=").split(args[1]);

        SparkConf conf = new SparkConf().setAppName("UserWatchDuration");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "1500");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaSparkContext context = ssc.sparkContext();


        Broadcast<String> projectBroadcast = context.broadcast(map.getOrDefault("project", "default"));
        Broadcast<Long> startTimeUBroadcast = context.broadcast(Long.parseLong(map.getOrDefault("startTimeU", "1539594752")));
        Broadcast<Long> endTimeUBroadcast = context.broadcast(Long.parseLong(map.getOrDefault("endTimeU", "1539594752")));
        Broadcast<List<String>> catesBroadcast = context.broadcast(Arrays.asList(map.getOrDefault("cates", "default").split("-")));

        Broadcast<Integer> dayMinutesBroadcast = context.broadcast(Integer.parseInt(map.getOrDefault("dayMinutes", "180")));

        if (map.containsKey("groupId")) {
            groupId = map.get("groupId");
        }
        if (map.containsKey("bootServers")) {
            bootServers = map.get("bootServers");
        }

        /**
         * 广播redis相关变量
         */
        if (map.containsKey("redisHost")) {
            redisHost = map.get("redisHost");
        }
        if (map.containsKey("redisPwd")) {
            redisPwd = map.get("redisPwd");
        }
        if (map.containsKey("redisPort")) {
            redisPort = Integer.parseInt(map.get("redisPort"));
        }
        Broadcast<String> redisHostBroadcast = context.broadcast(redisHost);
        Broadcast<Integer> redisPortBroadcast = context.broadcast(redisPort);
        Broadcast<String> redisPwdBroadcast = context.broadcast(redisPwd);
        System.out.println("redisHost:" + redisHost + ";redisPort:" + redisPort + ";redisPwd:" + redisPwd + ";project:" + projectBroadcast.getValue() + ";startTimeU:" + startTimeUBroadcast.getValue() + ";endTimeU:" + endTimeUBroadcast.getValue());

        /**
         * 广播流地址与房间号对应关系变量,暂时不用，等确定流地址与房间号一一对应之后再使用
         */
//        Map<String, String> streamRoomIdMap = getStreamRoomIdMap();
//        Broadcast<Map<String, String>> streamRoomIdMapBroadcast = context.broadcast(streamRoomIdMap);


        /**
         * 创建dstream(只处理client_online,player_online数据，其他的不接收)
         */
        JavaInputDStream<ConsumerRecord<String, String>> message = initMessage(ssc, args);

        /**
         * 添加切换版区实时数据，并且停掉切换版区进程，测试是否有问题//TODO
         */

        message.map(m -> m.value()).filter(l -> !l.contains("uid=0") && !l.contains("uid=-")).mapToPair(l -> {
            try {
                String flag = "p";//player_online
                int timeIndex = l.indexOf("[");
                String timeStr = l.substring(timeIndex + 1, l.indexOf("]", timeIndex));
                long timeU = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH).parse(timeStr).getTime() / 1000;
                long l1 = Long.parseLong(String.valueOf(timeU));
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
                if (l.contains("GET /client_punch.gif?")) {
                    flag = "c";
                    int roomIdIndex = l.indexOf("rid=");
                    int roomIdIndex1 = l.indexOf("&", roomIdIndex);
                    int roomIdIndex2 = l.indexOf(" ", roomIdIndex);
                    int roomIdEndIndex = 0;
                    if (roomIdIndex1 < 0) {
                        roomIdEndIndex = roomIdIndex2;
                    } else {
                        roomIdEndIndex = roomIdIndex1;
                    }
                    String roomId = l.substring(roomIdIndex + 4, roomIdEndIndex);
                    return new Tuple2<String, String>(roomId, uid + "-" + timeU + ":" + flag);
                }
                int uIndex = l.indexOf("u=");
                String url = l.substring(uIndex + 2, l.indexOf(".flv", uIndex));//此处不适合client_online
                String urlPart = url.split("live_panda%2F")[1];
                String stream = null;
                if (urlPart.contains("_")) {
                    stream = urlPart.split("_")[0];
                } else {
                    stream = urlPart;
                }
                return new Tuple2<String, String>(stream, uid + "-" + timeU + ":" + flag);
            } catch (NumberFormatException ne) {
//                logger.error("mapToPair,timeU parseLong error,l:" + l);
                return new Tuple2<String, String>(null, "-");
            } catch (Exception e) {
//                logger.error("mapToPair exception," + e.getMessage() + ",l:" + l);
                return new Tuple2<String, String>(null, "-");
            }
        }).filter(f -> {
            return StringUtils.isNotEmpty(f._1);
        }).filter(f -> {
            boolean result = false;
            long timeU = 0;
            try {
                timeU = Long.parseLong(f._2.split("-")[1].substring(0, 10));
                result = timeU >= startTimeUBroadcast.value() && timeU <= endTimeUBroadcast.value();
            } catch (StringIndexOutOfBoundsException se) {
//                logger.error("mapToPair,stream:" + f._1 + ";uid-timeU:flag-->" + (f._2));
//                logger.error("filter StringIndexOutOfBoundsException," + se.getMessage() + ";f._2:" + f._2);
            } catch (NumberFormatException ne) {
//                logger.error("filter NumberFormatException," + ne.getMessage() + ";f._2:" + f._2);
            } catch (Exception e) {
//                logger.error("filter Exception," + e.getMessage() + ";f._2:" + f._2);
            }
            return result;
        }).reduceByKey((a, b) ->/*按stream将{uid}-{timeU}{flag}变成多个按逗号分隔*/ new StringBuffer(a).append(",").append(b).toString()).mapPartitionsToPair(kv -> {
            /**
             * stream转换成roomId
             */
            Set<Tuple2<String, String>> res = new HashSet<>();
            Jedis jedis = null;
            try {
                jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                String redisPwd = redisPwdBroadcast.value();
                if (StringUtils.isNotEmpty(redisPwd)) {
                    jedis.auth(redisPwd);
                }
//            Map<String, String> streamRoomIdMapValue = streamRoomIdMapBroadcast.value();
                Map<String, String> streamMap = new HashMap<>();
                while (kv.hasNext()) {
                    Tuple2<String, String> next = kv.next();
                    String stream = next._1;
                    if (next._2.contains(":c")) {//是client_online
                        res.add(new Tuple2<String, String>(stream, next._2));
                        continue;
                    }
                    streamMap.put(stream, next._2);
//                if (streamRoomIdMapValue.containsKey(stream)) {
//                    res.add(new Tuple2<String, String>(streamRoomIdMapValue.get(stream), next._2));
//                } else {
//                    String roomId = jedis.get(new StringBuffer("stream:").append(stream).toString());
//                    if (StringUtils.isNotEmpty(roomId)) {
//                        res.add(new Tuple2<String, String>(roomId, next._2));
//                    }
//                }
                }
                List<String> streams = Lists.newArrayList(streamMap.keySet());
                List<String> streamKeys = streams.stream().map(s -> new StringBuffer("stream:").append(s).toString()).collect(Collectors.toList());

                List<String> roomIds = new ArrayList<>();
                if (streamKeys.size() > 0) {
                    roomIds = jedis.mget(streamKeys.toArray(new String[streamKeys.size()]));
                }
                for (int i = 0; i < streams.size(); i++) {
                    String stream = streams.get(i);
                    String roomId = roomIds.get(i);
                    if (StringUtils.isEmpty(roomId)) {
//                        logger.warn("stream:" + stream + ";对应的roomId:" + roomId);
                        continue;
                    }
                    res.add(new Tuple2<String, String>(roomId, streamMap.get(stream)));
                }
            } catch (Exception e) {
//                logger.error("mapPartitionsToPair," + e.getMessage());
            } finally {
                if (null != jedis) {
                    jedis.close();
                }
            }
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
//                    logger.warn("roomId:" + roomId + ",对应的切换版区数据在redis查询不到");
                } else {
                    List<String> catesValue = catesBroadcast.value();
                    for (int i = 0; i < uidTimeuArr.length; i++) {
                        String uidTime = uidTimeuArr[i];
                        String[] arr = uidTime.split("-");
                        String uid = arr[0];
                        Integer.parseInt(uid);
                        long logTimeU = Long.parseLong(arr[1].substring(0, 10));
                        String cate = null;
                        for (int j = 0; j < tuples.length; j++) {
                            Tuple tuple = tuples[j];
                            double score = tuple.getScore();
                            cate = tuple.getElement();
                            if (logTimeU >= score) {
                                break;
                            }
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
            } catch (StringIndexOutOfBoundsException se) {
//                logger.error(se.getMessage() + ";flatMapToPair,StringIndexOutOfBoundsException," + se.getMessage() + ";kv._2:" + kv._2);
            } catch (Exception e) {
//                e.printStackTrace();
//                logger.error(e.getMessage() + "flatMapToPair,Exception," + e.getMessage() + ";kv._2:" + kv._2);
//                logger.error(e.getMessage());
            } finally {
                if (null != jedis) {
                    jedis.close();
                }
            }
            return uidTimeuList.iterator();
        }).groupByKey().foreachRDD(rdd -> {
            rdd.foreachPartition(it -> {
                Jedis jedis = null;
                Producer<String, String> producer = null;
                String redisPwd = redisPwdBroadcast.value();
                try {
                    jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                    if (StringUtils.isNotEmpty(redisPwd)) {
                        jedis.auth(redisPwd);
                    }
                    Pipeline pipelined = jedis.pipelined();
                    Set<String> uids = new HashSet<>();//uids不能再foreachRDD内使用，foreachPartition相当于foreachRDD的匿名函数，不能修改uids
                    Set<String> keys = new HashSet<>();
                    while (it.hasNext()) {
                        Tuple2<String, Iterable<Long>> next = it.next();
                        String uid = next._1;
                        Iterator<Long> iterator = next._2.iterator();
                        SimpleDateFormat dayFormat = new SimpleDateFormat("yyyyMMdd");
                        while (iterator.hasNext()) {
                            Long next1 = iterator.next();//timeU
                            String day = dayFormat.format(new Date(next1 * 1000l));
                            String key = new StringBuffer(projectBroadcast.value()).append(":user:dur:pf:").append(uid).append(":").append(day).toString();
                            keys.add(key);
                            pipelined.pfadd(key, String.valueOf(next1));
                            pipelined.expire(key, 604800);//7天
                        }
                    }
                    pipelined.sync();
                    pipelined.close();

                    ObjectMapper mapper = null;
                    for (String key : keys) {
                        long pfcount = jedis.pfcount(key);
                        if (pfcount >= dayMinutesBroadcast.value()) {
                            String[] keySplit = key.split(":");
                            String uid = keySplit[keySplit.length - 2];
                            String userSingDaysKey = new StringBuffer(projectBroadcast.value()).append(":user:singin:days:").append(uid).toString();
                            Long sadd = jedis.sadd(userSingDaysKey, keySplit[keySplit.length - 1]);//{project}:user:singin:days:{uid}-->{day}每个用户签到的日期set
                            jedis.expire(userSingDaysKey, 5184000);//60天
                            if (sadd > 0) {//是新日期
                                Long scard = jedis.scard(userSingDaysKey);
                                if (null == producer) {
                                    producer = createProducer();
                                }
                                if (null == mapper) {
                                    mapper = new ObjectMapper();
                                }
                                logger.info("sadd>0,scard:" + scard + ";uid:" + uid);
//                                if (scard == 1 || scard == 5 || scard == 10 || scard == 20 || scard == 30) {
                                HashMap<Object, Object> sendMap = Maps.newHashMap();
                                sendMap.put("uid", uid);
                                sendMap.put("days", scard);
                                String value = mapper.writeValueAsString(sendMap);
                                RecordMetadata metadata = producer.send(new ProducerRecord<>("pcgameq_panda_watch_time_sign", value)).get();
                                HashMap<Object, Object> metaMap = Maps.newHashMap();
                                metaMap.put("patition", metadata.partition());
                                metaMap.put("offset", metadata.offset());
                                metaMap.put("topic", metadata.topic());
                                metaMap.put("days", scard);
                                metaMap.put("timeU", new Date().getTime());
                                String metaJson = mapper.writeValueAsString(metaMap);
                                String sendsKey = new StringBuffer(projectBroadcast.value()).append(":kafkasends").toString();
                                jedis.hset(sendsKey, new StringBuffer(uid).toString(), metaJson);
//                                System.out.println(new StringBuffer("time:").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append(";value:").append(value).append(";metaJson:").append(metaJson).toString());

//                                }
                                jedis.expire(sendsKey, 5184000);//60天

                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("foreachRDD:" + e.getMessage());
                } finally {
                    if (null != jedis) {
                        jedis.close();
                    }
                    if (null != producer) {
                        producer.close();
                    }
                }
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }

    private static Producer<String, String> createProducer() {
        //spark.streaming.kafka.maxRatePerPartition
        Properties props = new Properties();
        props.put("bootstrap.servers", targetBootServers);
        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("transactional.id", transactionalId);
        props.put("batch.size", 5);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 3554432);
//        props.put("enable.idempotence", true);//幂等性，retries不再设置，默认int最大值，版本必须0.11以后的，项目是0.10，因此不支持
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private static JavaInputDStream<ConsumerRecord<String, String>> initMessage(JavaStreamingContext ssc, String[] args) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
//        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("auto.offset.reset", "latest");
        //batch duration大于30秒，需设置以下两个参数
//        kafkaParams.put("heartbeat.interval.ms", "130000");//小于session.timeout.ms，最后高于1/3
//        kafkaParams.put("session.timeout.ms", "300000");//范围group.min.session.timeout.ms(6000)与group.max.session.timeout.ms(30000)之间
//        kafkaParams.put("request.timeout.ms", "310000");
        //batch duration大于5分钟，需在broker设置group.max.session.timeout.ms参数
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList(args[0].split(","));

        return KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
    }


}
