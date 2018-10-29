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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author: likaiqing
 * @create: 2018-10-22 14:17
 * 新用户及修改头像昵称数据需要实时或者每小时更新
 **/
public class RankGift {

    private static final Logger logger = LogManager.getLogger(UserWatchDuration.class);

    //    private static String topics = "panda_present_detail_test1";
    private static String topics = "pcgameq_panda_gift_donate";
    private static String groupId = "gift_rank_stream_test";
    //test
//    private static String bootServers = "10.131.6.79:9092";
    private static String bootServers = "10.131.7.20:9092";
    //test
//    private static String bootServers = "10.131.7.20:9092,10.131.7.31:9092,10.131.7.25:9092";
    //online
//    private static String bootServers = "10.131.4.106:9092,10.131.4.108:9092,10.131.11.64:9092,10.131.11.117:9092";

    private static String projectKey = "rank:gift:projectMap";

    private static String redisHost = "localhost";
    private static String redisPwd = "";
    private static int redisPort = 6379;

//    private static String redisHost = "10.131.7.48";
//    private static String redisPwd = "";
//    private static int redisPort = 6099;

//    private static String redisHost = "10.131.11.151";
//    private static String redisPwd = "Hdx03DqyIwOSrEDU";
//    private static int redisPort = 6974;


    private static Broadcast<Map<String, RankProject>> projectBcs;

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
        conf.set("spark.streaming.kafka.maxRatePerPartition", "30");
        /**
         * //TODO 使用checkpoint
         */
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaSparkContext context = ssc.sparkContext();
        Broadcast<String> redisHostBroadcast = context.broadcast(redisHost);
        Broadcast<Integer> redisPortBroadcast = context.broadcast(redisPort);
        Broadcast<String> redisPwdBroadcast = context.broadcast(redisPwd);

        /**
         * 初次更新榜单项目广播变量
         */
        InitPojectsBc initPojectsBc = new RankGift().new InitPojectsBc(context);
        initPojectsBc.run();
        /**
         * 定期更新广播变量，有新项目加入，1天更新两次即可
         */
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleWithFixedDelay(initPojectsBc, 10, 10, TimeUnit.SECONDS);
        JavaInputDStream<ConsumerRecord<String, String>> message = initMessage(ssc, args);
        /**
         * 用户修改昵称，新用户等实时数据,主播实时数据 //TODO
         */
        message.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            try {
                rdd.map(r -> r.value()).foreachPartition(p -> {
                    String threadName = Thread.currentThread().getName();
                    OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                    if (o.fromOffset() != o.untilOffset()) {
                        System.out.println(threadName + ";" + o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                    }
                    Jedis jedis = null;
                    Map<String, String> qidRoomIdMap = new HashMap<>();
                    Set<String> uids = new HashSet<>();
                    jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                    if (StringUtils.isNotEmpty(redisPwdBroadcast.value())) {
                        jedis.auth(redisPwdBroadcast.value());
                    }
                    ObjectMapper mapper = new ObjectMapper();
                    while (p.hasNext()) {
                        String next = p.next();
                        GiftInfo giftInfo = getGiftInf(next, mapper);
                        if (null == giftInfo) continue;
                        DateTime dateTime = new DateTime(giftInfo.getTimeU() * 1000l);
                        String day = DateTimeFormat.forPattern("yyyyMMdd").print(dateTime);
                        int week = dateTime.weekOfWeekyear().get();
                        uids.add(giftInfo.getUid());
                        for (Map.Entry<String, RankProject> entry : projectBcs.value().entrySet()) {
                            executeSingleProject(jedis, entry, giftInfo, qidRoomIdMap, giftInfo.getQid(), threadName, day, week);
                        }
                    }
                    for (String uid : uids) {
                        String key = "panda:zhaomu:userdetail:" + uid;
                        if (!jedis.exists(key)) {
                            Map<String, String> userMap = new HashMap<>();
                            String detail = getUserDetailMap(uid);
                            JsonNode data = mapper.readTree(mapper.readTree(detail).get("data").asText());
                            userMap.put("nickName", data.get("nickName").asText());
                            userMap.put("avatar", data.get("avatar").asText());
                            jedis.set(key, mapper.writeValueAsString(userMap), "NX", "EX", 2592000);
                        }
                    }
                    for (Map.Entry<String, String> entry : qidRoomIdMap.entrySet()) {
                        String qid = entry.getKey();
                        String key = "panda:zhaomu:anchordetail:" + qid;
                        if (!jedis.exists(key)) {
                            Map<String, String> anchorMap = new HashMap<>();
                            String roomId = entry.getValue();
                            String detail = getUserDetailMap(qid);
                            JsonNode data = mapper.readTree(mapper.readTree(detail).get("data").asText());
                            anchorMap.put("roomId", roomId);
                            anchorMap.put("nickName", data.get("nickName").asText());
                            anchorMap.put("avatar", data.get("avatar").asText());
                            jedis.set(key, mapper.writeValueAsString(anchorMap), "NX", "EX", 2592000);
                        }
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
            System.out.println(next);
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

    private static void executeSingleProject(Jedis jedis, Map.Entry<String, RankProject> entry, GiftInfo giftInfo, Map<String, String> qidRoomidMap, String qid, String threadName, String day, int week) {
        RankProject rankProject = entry.getValue();
        long startTimeU = rankProject.getStartTimeU();
        long endTimeU = rankProject.getEndTimeU();
        long timeU = giftInfo.getTimeU();
        String cate = giftInfo.getCate();
        String total = giftInfo.getTotal();
        String roomId = giftInfo.getRoomId();
        String giftId = giftInfo.getGiftId();
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
        /**
         * 只有分组统计的情况下，才使用广播变量里的qids和anchor2GroupMap
         */
        String group = rankProject.getAnchor2GroupMap().get(qid);
        if (rankProject.getFlag() == 2 && (!rankProject.getQids().contains(qid) || StringUtils.isEmpty(group))) {
            return;
        }

        if (rankProject.isAllRank()) {
            setGift(rankProject, qid, jedis, threadName, total, "anchorAllGift");//设置总礼物榜单
            setGift(rankProject, qid, jedis, threadName, total, "userAllGift");//设置总礼物榜单
            qidRoomidMap.put(qid, roomId);
        }
        if (rankProject.isSpecificRank() && rankProject.getGiftIds().contains(giftId)) {
            setGift(rankProject, qid, jedis, threadName, total, "anchorSpecificGift");//设置特殊礼物榜单
            setGift(rankProject, qid, jedis, threadName, total, "userSpecificGift");//设置特殊礼物榜单
            qidRoomidMap.put(qid, roomId);
        }
        if (rankProject.isDayAllRank()) {
            setGift(rankProject, qid, jedis, threadName, total, "anchorDayAllGift" + day);//设置总礼物榜单
            setGift(rankProject, qid, jedis, threadName, total, "userDayAllGift" + day);//设置总礼物榜单
            qidRoomidMap.put(qid, roomId);
        }
        if (rankProject.isDaySpecificRank() && rankProject.getGiftIds().contains(giftId)) {
            setGift(rankProject, qid, jedis, threadName, total, "anchorDaySpecificGift" + day);//设置总礼物榜单
            setGift(rankProject, qid, jedis, threadName, total, "userDaySpecificGift" + day);//设置总礼物榜单
            qidRoomidMap.put(qid, roomId);
        }
        if (rankProject.isWeekAllRank()) {
            setGift(rankProject, qid, jedis, threadName, total, "anchorWeekAllGift" + week);//设置总礼物榜单
            setGift(rankProject, qid, jedis, threadName, total, "userWeekAllGift" + week);//设置总礼物榜单
            qidRoomidMap.put(qid, roomId);
        }
        if (rankProject.isWeekSpecificRank() && rankProject.getGiftIds().contains(giftId)) {
            setGift(rankProject, qid, jedis, threadName, total, "anchorWeekSpecificGift" + week);//设置总礼物榜单
            setGift(rankProject, qid, jedis, threadName, total, "userWeekSpecificGift" + week);//设置总礼物榜单
            qidRoomidMap.put(qid, roomId);
        }
        if (rankProject.getFlag() == 2) {//按分组报名方式
            if (rankProject.isAllRank()) {
                setGift(rankProject, qid, jedis, threadName, total, "anchorGroupAllGift" + group);//设置总礼物榜单
                setGift(rankProject, qid, jedis, threadName, total, "userGroupAllGift");//设置总礼物榜单
                qidRoomidMap.put(qid, roomId);
            }
            if (rankProject.isSpecificRank()) {
                setGift(rankProject, qid, jedis, threadName, total, "anchorGroupSpecificGift" + group);//设置总礼物榜单
                setGift(rankProject, qid, jedis, threadName, total, "userGroupSpecificGift");//设置总礼物榜单
                qidRoomidMap.put(qid, roomId);
            }
            if (rankProject.isDayAllRank()) {
                setGift(rankProject, qid, jedis, threadName, total, "anchorGroupDayAllGift" + group);//设置总礼物榜单
                setGift(rankProject, qid, jedis, threadName, total, "userGroupDayAllGift");//设置总礼物榜单
                qidRoomidMap.put(qid, roomId);
            }
            if (rankProject.isDaySpecificRank()) {
                setGift(rankProject, qid, jedis, threadName, total, "anchorGroupDaySpecificGift" + group);
                setGift(rankProject, qid, jedis, threadName, total, "userGroupDaySpecificGift");
                qidRoomidMap.put(qid, roomId);
            }
            if (rankProject.isDayAllRank()) {
                setGift(rankProject, qid, jedis, threadName, total, "anchorGroupWeekAllGift" + group);//设置总礼物榜单
                setGift(rankProject, qid, jedis, threadName, total, "userGroupDayWeekGift");//设置总礼物榜单
                qidRoomidMap.put(qid, roomId);
            }
            if (rankProject.isDaySpecificRank()) {
                setGift(rankProject, qid, jedis, threadName, total, "anchorGroupWeekSpecificGift" + group);
                setGift(rankProject, qid, jedis, threadName, total, "userGroupWeekSpecificGift");
                qidRoomidMap.put(qid, roomId);
            }
        }
    }

    private static void setGift(RankProject rankProject, String qid, Jedis jedis, String threadName, String total, String giftType) {
        String key = new StringBuffer("panda:").append(rankProject.getProject()).append(":").append(giftType).append(":lock:").append(qid).toString();
        while (null == jedis.set(key, threadName, "NX", "PX", 1000)) {
            try {
                logger.warn("设置key:" + key + ";已经存在，等待50毫秒");
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long allGift = jedis.hincrBy(new StringBuffer("panda:").append(rankProject.getProject()).append(":").append(giftType).append(":map").toString(), qid, Long.parseLong(total));
        String rankKey = new StringBuffer("panda:").append(rankProject.getProject()).append(":").append(giftType).append(":rank").toString();
        jedis.zadd(rankKey, allGift, qid);
        jedis.expire(rankKey, 2592000);//30天过期
//        boolean need = false;
//        if (rankProject.getFlag() == 1) {//报名的方式
//            if (jedis.sismember(new StringBuffer("hostpool-").append(rankProject.getProject()).toString(), qid)) {//报名列表包含此主播，将其添加到，添加到统计当中,
//                need = true;
//            }
//        } else if (rankProject.getFlag() == 2) {
//            if (giftType.contains("roupGift")) {
//                String group = giftType.substring(giftType.indexOf("roupGift") + "roupGift".length());
//                if (jedis.sismember(new StringBuffer("hostpool-").append(rankProject.getProject()).append("-group-").append(group).toString(), qid)) {
//                    need = true;
//                }
//            }
//        } else if (rankProject.getFlag() == 0) {//按开播统计的，执行到此处的肯定是时间和版区都符合要求的
//            need = true;
//        }
//        if (need) {
//            jedis.zadd(rankKey, allGift, qid);
//            jedis.expire(rankKey, 2592000);//30天过期
//        }
        if (null != jedis.get(key) && jedis.get(key).equals(threadName)) {
            jedis.del(key);
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


    /**
     * 更新广播变量projectBcs
     * reids存储,rank:gift:projectMap,field为project,value:参数字符串
     */
    class InitPojectsBc implements Runnable {
        JavaSparkContext context;

        public InitPojectsBc(JavaSparkContext context) {
            this.context = context;
        }

        @Override
        public void run() {
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
                        rankProject.setAllRank(Boolean.getBoolean(paramMap.get("allRank")));
                    }
                    if (paramMap.containsKey("specificRank")) {
                        rankProject.setSpecificRank(Boolean.getBoolean(paramMap.get("specificRank")));
                    }
                    if (paramMap.containsKey("hourAllRank")) {
                        rankProject.setHourAllRank(Boolean.getBoolean(paramMap.get("hourAllRank")));
                    }
                    if (paramMap.containsKey("dayAllRank")) {
                        rankProject.setDayAllRank(Boolean.getBoolean(paramMap.get("dayAllRank")));
                    }
                    if (paramMap.containsKey("weekAllRank")) {
                        rankProject.setWeekAllRank(Boolean.getBoolean(paramMap.get("weekAllRank")));
                    }
                    if (paramMap.containsKey("hourSpecificRank")) {
                        rankProject.setHourSpecificRank(Boolean.getBoolean(paramMap.get("hourSpecificRank")));
                    }
                    if (paramMap.containsKey("daySpecificRank")) {
                        rankProject.setDaySpecificRank(Boolean.getBoolean(paramMap.get("daySpecificRank")));
                    }
                    if (paramMap.containsKey("weekSpecificRank")) {
                        rankProject.setWeekSpecificRank(Boolean.getBoolean(paramMap.get("weekSpecificRank")));
                    }

                    int flag = Integer.parseInt(paramMap.get("flag"));
                    if (flag == 1) {//不需要初始化，实时判断
//                        rankProject.setQids(jedis.smembers("hostpool:" + key));
                    } else if (flag == 2) {
                        //TODO 分组的话，在redis中使用map结构，field:qid,value:group
                        Map<String, String> qid2GroupMap = jedis.hgetAll("hostmap:" + entry.getKey());
                        rankProject.setQids(qid2GroupMap.keySet());
                        rankProject.setAnchor2GroupMap(qid2GroupMap);
                    }
                    projectsMap.put(key, rankProject);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            jedis.close();
            if (null == projectBcs || projectBcs.value().size() == 0) {
                projectBcs = context.broadcast(projectsMap);
                logger.info("null == projectBcs || projectBcs.value().size() == 0");
            } else {
                boolean equals = true;
                for (Map.Entry<String, RankProject> entry : projectsMap.entrySet()) {
                    String project = entry.getKey();
                    RankProject rankProject = entry.getValue();
                    if (!projectBcs.value().containsKey(project) || !projectBcs.value().get(project).equals(rankProject)) {
                        equals = false;
                        break;
                    }
                }
                if (!equals || projectBcs.value().size() != projectMap.size()) {
                    projectBcs.unpersist(true);
                    projectBcs = context.broadcast(projectsMap);
                    logger.info("broadcast projectsMap");
                }
            }
        }
    }

}
