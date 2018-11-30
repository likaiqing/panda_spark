package com.pandatv.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.pandatv.bean.GiftInfo;
import com.pandatv.bean.RankProject;
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
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * @author: likaiqing
 * @create: 2018-10-22 14:17
 * 新用户及修改头像昵称数据需要实时或者每小时更新
 **/
public class RankGift {

    private static final Logger logger = LogManager.getLogger(RankGift.class);

    private static Broadcast<String> projectKeyBroadcast;

    //测试环境
//    private static String topics = "pcgameq_panda_gift_donate";
//    private static String groupId = "gift_rank_stream_test";
//    private static String bootServers = "10.131.7.20:9092,10.131.7.31:9092,10.131.7.25:9092";//测试环境消费礼物地址(t10-12v.infra.bjtb.pdtv.it) KM:http://t12v.infra.bjtb.pdtv.it:9090/clusters/beta_bjtb
//    private static String redisHost = "10.131.7.48";
//    private static String redisPwd = "";
//    private static int redisPort = 6099;

    //test ckafka
//    private static String topics = "panda_present_detail_test_1";
//    private static String groupId = "gift_rank_stream_test";
//    private static String bootServers = "10.131.6.79:9092";
//    private static String redisHost = "localhost";
//    private static String redisPwd = "";
//    private static int redisPort = 6379;

    //线上
    private static String topics = "pcgameq_panda_gift_donate";
    private static String bootServers = "10.131.10.27:9092";//kafkabiz6-10v.infra.bjtb.pdtv.it，worker服务器需要配置hosts KM:http://kafkabiz10v.infra.bjtb.pdtv.it:9090/clusters/online_bjtb_biz/consumers
    private static String groupId = "gift_rank_stream";
    private static String redisHost = "10.131.11.151";
    private static String redisPwd = "Hdx03DqyIwOSrEDU";
    private static int redisPort = 6974;


    /**
     * 指定topic即可，榜单项目通过redis进行配置，定期去更新项目广播变量,cates为空的话，代表所有版区
     * project=[project],startTimeU=[startTimeU],endTimeU=[endTimeU],cates=[cate1-cate2],giftIds=[giftId1-giftId2],hourRank=true,dayRank=true,weekRank=true,flag=[0|1|2]
     * <p>
     * 按开播的方式，只要符合时间和版区即可统计；报名的方式(固定主播名单类似)，从报名之后开始统计;分组固定主播名单
     *
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {
        if (args.length == 1) {
            topics = args[0].trim();
        }
        if (args.length == 2) {
            Map<String, String> map = Splitter.on(",").withKeyValueSeparator("=").split(args[1]);
            initParams(map);
        }
        SparkConf conf = new SparkConf().setAppName("rank_gift");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "100");
        /**
         * //TODO 使用checkpoint
         */
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaSparkContext context = ssc.sparkContext();

        Broadcast<String> redisHostBroadcast = context.broadcast(redisHost);
        Broadcast<Integer> redisPortBroadcast = context.broadcast(redisPort);
        Broadcast<String> redisPwdBroadcast = context.broadcast(redisPwd);
        projectKeyBroadcast = context.broadcast("rank:gift:projectMap");

        JavaInputDStream<ConsumerRecord<String, String>> message = initMessage(ssc, args);

        message.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            try {
                Map<String, RankProject> rankProjectMap = getProjectMap();
                rdd.map(r -> r.value()).foreachPartition(p -> {
                    if (null == rankProjectMap || rankProjectMap.size() == 0) {
                        return;
                    }
                    OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//                    if (o.fromOffset() != o.untilOffset()) {
//                        System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
//                    }
                    Jedis jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                    if (StringUtils.isNotEmpty(redisPwdBroadcast.value())) {
                        jedis.auth(redisPwdBroadcast.value());
                    }
                    ObjectMapper mapper = new ObjectMapper();
                    List<Tuple3<String, Double, String>> result = new ArrayList<>();
                    while (p.hasNext()) {
                        String next = p.next();
                        logger.info("next:" + next);
                        GiftInfo giftInfo = getGiftInf(next, mapper);
                        if (null == giftInfo || Integer.parseInt(giftInfo.getTotal()) <= 0) continue;
                        DateTime dateTime = new DateTime(giftInfo.getTimeU() * 1000l);
                        String day = DateTimeFormat.forPattern("yyyyMMdd").print(dateTime);
                        int week = dateTime.weekOfWeekyear().get();
                        for (Map.Entry<String, RankProject> entry : rankProjectMap.entrySet()) {
                            logger.info("executeSingleProject,project:" + entry.getValue());
                            executeSingleProject(jedis, entry, giftInfo, giftInfo.getQid(), day, week, result);
                        }
                    }
                    Pipeline pipelined = jedis.pipelined();
                    for (Tuple3<String, Double, String> tuple3 : result) {
                        pipelined.zincrby(tuple3._1(), tuple3._2(), tuple3._3());
                    }
                    pipelined.sync();
                    if (null != pipelined) {
                        pipelined.close();
                    }
                    if (null != jedis) {
                        jedis.close();
                    }
                });
                ((CanCommitOffsets) message.inputDStream()).commitAsync(offsetRanges);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        ssc.start();
        ssc.awaitTermination();

    }

    private static Map<String, RankProject> getProjectMap() {
        Jedis jedis = new Jedis(redisHost, redisPort);
        if (StringUtils.isNotEmpty(redisPwd)) {
            jedis.auth(redisPwd);
        }
        Map<String, RankProject> projectsMap = new HashMap<>();
        Map<String, String> projectMap = jedis.hgetAll(projectKeyBroadcast.getValue());
//        logger.warn("InitPojectsBc run");
        for (Map.Entry<String, String> entry : projectMap.entrySet()) {
            try {
                String key = entry.getKey();
                String value = entry.getValue();
                Map<String, String> paramMap = Splitter.on(",").withKeyValueSeparator("=").split(value);
                if (!paramMap.containsKey("project") || !paramMap.containsKey("startTimeU") || !paramMap.containsKey("endTimeU") || !paramMap.containsKey("flag")) {
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
                if (paramMap.containsKey("giftIds")) {
                    String giftIds = paramMap.get("giftIds");
                    if (StringUtils.isNotEmpty(giftIds)) {
                        List<String> giftList = Arrays.asList(giftIds.split("-"));
                        rankProject.setGiftIds(giftList);
                    }
                }
                if (paramMap.containsKey("allRank")) {
                    rankProject.setAllRank(Boolean.parseBoolean(paramMap.get("allRank")));
                }
                if (paramMap.containsKey("specificRank")) {
                    rankProject.setSpecificRank(Boolean.parseBoolean(paramMap.get("specificRank")));
                }
                if (paramMap.containsKey("hourAllRank")) {
                    rankProject.setHourAllRank(Boolean.parseBoolean(paramMap.get("hourAllRank")));
                }
                if (paramMap.containsKey("dayAllRank")) {
                    rankProject.setDayAllRank(Boolean.parseBoolean(paramMap.get("dayAllRank")));
                }
                if (paramMap.containsKey("weekAllRank")) {
                    rankProject.setWeekAllRank(Boolean.parseBoolean(paramMap.get("weekAllRank")));
                }
                if (paramMap.containsKey("monthAllRank")) {
                    rankProject.setMonthAllRank(Boolean.parseBoolean(paramMap.get("monthAllRank")));
                }
                if (paramMap.containsKey("hourSpecificRank")) {
                    rankProject.setHourSpecificRank(Boolean.parseBoolean(paramMap.get("hourSpecificRank")));
                }
                if (paramMap.containsKey("daySpecificRank")) {
                    rankProject.setDaySpecificRank(Boolean.parseBoolean(paramMap.get("daySpecificRank")));
                }
                if (paramMap.containsKey("weekSpecificRank")) {
                    rankProject.setWeekSpecificRank(Boolean.parseBoolean(paramMap.get("weekSpecificRank")));
                }
                if (paramMap.containsKey("monthSpecificRank")) {
                    rankProject.setMonthSpecificRank(Boolean.parseBoolean(paramMap.get("monthSpecificRank")));
                }
                //特殊礼物额外多加
                if (paramMap.containsKey("specificExtraAdd")) {
                    rankProject.setSpecificExtraAdd(Boolean.parseBoolean(paramMap.get("specificExtraAdd")));
                }
                if (paramMap.containsKey("specificExtraRate")) {
                    rankProject.setSpecificExtraRate(Double.parseDouble(paramMap.get("specificExtraRate")));
                }

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

    private static String getUserDetailMap(String uid) {
        String userInfoUrlPre = "http://u.pdtv.io:8360/profile/getProfileByRid?rid=";
        BufferedReader br = null;
        StringBuffer result = new StringBuffer();
        try {
            URL url = new URL(userInfoUrlPre + uid);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.connect();
            String line;
            br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            while ((line = br.readLine()) != null) {
                result.append(line);
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result.toString();
    }

    private static GiftInfo getGiftInf(String next, ObjectMapper mapper) {
        GiftInfo giftInfo = null;
        try {
            JsonNode jsonNode = mapper.readTree(next);
            String dataStr = jsonNode.get("data").asText();
            JsonNode data = mapper.readTree(dataStr);
            String uid = data.get("uid").asText();
            String qid = data.get("anchor").asText();
            String roomId = data.get("roomid").asText();
            String giftId = data.get("giftid").asText();
            String total = data.get("total").asText();
            String cate = data.get("cate").asText();
            long timeU = data.get("time").asLong();
            giftInfo = new GiftInfo();
            giftInfo.setUid(uid);
            giftInfo.setQid(qid);
            giftInfo.setRoomId(roomId);
            giftInfo.setGiftId(giftId);
            giftInfo.setCate(cate);
            giftInfo.setTotal(total);
            giftInfo.setTimeU(timeU);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("next:" + next);
        }
        return giftInfo;
    }

    private static void executeSingleProject(Jedis jedis, Map.Entry<String, RankProject> entry, GiftInfo giftInfo, String qid, String day, int week, List<Tuple3<String, Double, String>> result) throws IOException {
        RankProject rankProject = entry.getValue();
        long startTimeU = rankProject.getStartTimeU();
        long endTimeU = rankProject.getEndTimeU();
        long timeU = giftInfo.getTimeU();
        String cate = giftInfo.getCate();
        String total = giftInfo.getTotal();
        String roomId = giftInfo.getRoomId();
        String giftId = giftInfo.getGiftId();
        String uid = giftInfo.getUid();
        String month = day.substring(0, 6);
        if (timeU < startTimeU || timeU > endTimeU) {
            return;
        }
        if (rankProject.getCates().size() > 0) {
            if (!rankProject.getCates().contains(cate)) {
                return;//版区列表不为空，并且不包含此版区，过滤
            }
        }
        /**
         * 只要不是按开播统计(主播固定的方式)统计，都是以主播加入列表的时间统计,computeNew
         */
        if (rankProject.getFlag() == 1 && !jedis.sismember("hostpool:" + rankProject.getProject(), qid)) {//报名或者提供主播列表方式
            return;
        }
        String group = null;
        if (rankProject.getFlag() == 2) {
            if (!jedis.hexists("hostmap:" + rankProject.getProject(), qid)) {
                return;
            }
            group = jedis.hget("hostmap:" + rankProject.getProject(), qid);
            if (StringUtils.isEmpty(group)) {
                return;
            }
        }
        double tmpTotal = Double.parseDouble(total);
        try {
            if (rankProject.isSpecificExtraAdd() && rankProject.getGiftIds().size() > 0 && rankProject.getGiftIds().contains(giftId) && rankProject.getSpecificExtraRate() > 0) {
                tmpTotal = tmpTotal * (1 + rankProject.getSpecificExtraRate());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (rankProject.isAllRank()) {
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancAlGf").append(":rank").toString(), tmpTotal, qid));
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrAlGf").append(":rank").toString(), tmpTotal, uid));
        }
        if (rankProject.isSpecificRank() && rankProject.getGiftIds().contains(giftId)) {
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancSpecGf").append(":rank").toString(), tmpTotal, qid));
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrSpecGf").append(":rank").toString(), tmpTotal, uid));
        }
        if (rankProject.isDayAllRank()) {
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancDyAlGf").append(day).append(":rank").toString(), tmpTotal, qid));
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrDyAlGf").append(day).append(":rank").toString(), tmpTotal, uid));
        }
        if (rankProject.isDaySpecificRank() && rankProject.getGiftIds().contains(giftId)) {
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancDySpecGf").append(day).append(":rank").toString(), tmpTotal, qid));
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrDySpecGf").append(day).append(":rank").toString(), tmpTotal, uid));
        }
        if (rankProject.isWeekAllRank()) {
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancWkAlGf").append(week).append(":rank").toString(), tmpTotal, qid));
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrWkAlGf").append(week).append(":rank").toString(), tmpTotal, uid));
        }
        if (rankProject.isWeekSpecificRank() && rankProject.getGiftIds().contains(giftId)) {
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancWkSpecGf").append(week).append(":rank").toString(), tmpTotal, qid));
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrWkSpecGf").append(week).append(":rank").toString(), tmpTotal, uid));
        }
        if (rankProject.isMonthAllRank()) {
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancMthAlGf").append(month).append(":rank").toString(), tmpTotal, qid));
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrMthAlGf").append(month).append(":rank").toString(), tmpTotal, uid));
        }
        if (rankProject.isMonthSpecificRank() && rankProject.getGiftIds().contains(giftId)) {
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancMthSpecGf").append(month).append(":rank").toString(), tmpTotal, qid));
            result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrMthSpecGf").append(month).append(":rank").toString(), tmpTotal, uid));
        }

        if (rankProject.getFlag() == 2) {//按分组报名方式
            if (rankProject.isAllRank()) {
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancGrp" + group + "AlGf").append(":rank").toString(), tmpTotal, qid));
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrGrp" + group + "AlGf").append(":rank").toString(), tmpTotal, uid));
            }
            if (rankProject.isSpecificRank()) {
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancGrp" + group + "SpecGf").append(":rank").toString(), tmpTotal, qid));
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrGrp" + group + "SpecGf").append(":rank").toString(), tmpTotal, uid));
            }
            if (rankProject.isDayAllRank()) {
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancGrp" + group + "DyAlGf" + day).append(":rank").toString(), tmpTotal, qid));
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrGrp" + group + "DyAlGf" + day).append(":rank").toString(), tmpTotal, uid));
            }
            if (rankProject.isDaySpecificRank()) {
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancGrp" + group + "DySpecGf" + day).append(":rank").toString(), tmpTotal, qid));
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrGrp" + group + "DySpecGf" + day).append(":rank").toString(), tmpTotal, uid));
            }
            if (rankProject.isWeekAllRank()) {
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancGrp" + group + "WkAlGf" + week).append(":rank").toString(), tmpTotal, qid));
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrGrp" + group + "WkAlGf" + week).append(":rank").toString(), tmpTotal, uid));
            }
            if (rankProject.isWeekSpecificRank()) {
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancGrp" + group + "WkSpecGf" + week).append(":rank").toString(), tmpTotal, qid));
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrGrp" + group + "WkSpecGf" + week).append(":rank").toString(), tmpTotal, uid));
            }
            if (rankProject.isMonthAllRank()) {
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancGrp" + group + "MthAlGf" + month).append(":rank").toString(), tmpTotal, qid));
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrGrp" + group + "MthAlGf" + month).append(":rank").toString(), tmpTotal, uid));
            }
            if (rankProject.isMonthSpecificRank()) {
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("ancGrp" + group + "MthSpecGf" + month).append(":rank").toString(), tmpTotal, qid));
                result.add(new Tuple3<>(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append("usrGrp" + group + "MthSpecGf" + month).append(":rank").toString(), tmpTotal, uid));
            }
        }
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
        if (map.containsKey("bootServers")) {
            bootServers = map.get("bootServers");
        }
        logger.info("groupId:" + groupId);
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

}
