package com.pandatv.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.pandatv.bean.RankProject;
import com.pandatv.bean.ShadowPopularity;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
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
import org.joda.time.format.DateTimeFormatter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Tuple3;

import java.io.IOException;
import java.util.*;

/**
 * @author: likaiqing
 * @create: 2018-11-21 11:28
 * <p>
 * {"data":"{\"classification\":\"starface\",\"hostid\":\"82436604\",\"person_num\":184,\"roomid\":\"4235606\",\"timestamp\":1542618451}","host":"pt9v.plat.bjtb.pdtv.it","key":"","name":"shadow_show_person_num","requestid":"","time":"2018-11-19 17:07:31"}
 **/
public class RankPopular {
    private static final Logger logger = LogManager.getLogger(RankPopular.class);

    private static String maxRatePerPartition = "500";
    private static String name;

    //测试环境
//    private static String redisHost = "10.131.7.48";
//    private static String redisPwd = "";
//    private static int redisPort = 6099;

    private static Broadcast<String> projectKeyBroadcast;

    //线上
    private static String topics = "pcgameq_shadow_show_person_num";
    private static String bootServers = "10.131.12.126:9092";//kafkabiz1-5v.infra.bjtb.pdtv.it，worker服务器需要配置hosts KM：http://kafkabiz3v.infra.bjtb.pdtv.it:9090/clusters/online_bjtb_biz/topics
    private static String groupId = "popular_rank_stream";
    private static String redisHost = "10.131.11.151";
    private static String redisPwd = "Hdx03DqyIwOSrEDU";
    private static int redisPort = 6974;

    public static void main(String[] args) throws InterruptedException {
        if (args.length == 1) {
            topics = args[0].trim();
        }
        if (args.length == 2) {
            topics = args[0].trim();
            Map<String, String> map = Splitter.on(",").withKeyValueSeparator("=").split(args[1]);
            initParams(map);
        }
        Jedis jedis1 = new Jedis(redisHost, redisPort);
        if (StringUtils.isNotEmpty(redisPwd)) {
            jedis1.auth(redisPwd);
        }
        String sha = jedis1.scriptLoad("local res=0 if redis.call('HEXISTS',KEYS[1],ARGV[1])==1 then local payload=redis.call('HGET',KEYS[1],ARGV[1]) if tonumber(ARGV[2])>tonumber(payload) then redis.call('HSET',KEYS[1],ARGV[1],ARGV[2]) res=1 else res=0 end else redis.call('HSET',KEYS[1],ARGV[1],ARGV[2]) res=1 end return res");
        jedis1.close();
        System.out.println("sha:" + sha);
        SparkConf conf = new SparkConf().setAppName(name);
        conf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaSparkContext context = ssc.sparkContext();

        Broadcast<String> redisHostBroadcast = context.broadcast(redisHost);
        Broadcast<Integer> redisPortBroadcast = context.broadcast(redisPort);
        Broadcast<String> redisPwdBroadcast = context.broadcast(redisPwd);
        Broadcast<String> shaBroadcast = context.broadcast(sha);
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
                    Jedis jedis = new Jedis(redisHostBroadcast.value(), redisPortBroadcast.value());
                    if (StringUtils.isNotEmpty(redisPwdBroadcast.value())) {
                        jedis.auth(redisPwdBroadcast.value());
                    }
                    ObjectMapper mapper = new ObjectMapper();
                    List<Tuple3<String, String, String>> result = new ArrayList<>();
                    DateTimeFormatter parse = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                    DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMdd");
                    Map<String, String> qidRoomIdMap = new HashedMap();
//                Pipeline pipelined = jedis.pipelined();
                    while (p.hasNext()) {
                        String next = p.next();
                        logger.info("next:" + next);
                        ShadowPopularity sp = getShadowPopularity(next, mapper);
                        qidRoomIdMap.put(sp.getHostId(), sp.getRoomId());
                        for (Map.Entry<String, RankProject> entry : rankProjectMap.entrySet()) {
                            logger.info("executeSingleProject,project:" + entry.getValue());
                            executeSinglePojectPopular(jedis, entry, sp, result, parse, format);
                        }
                    }
                    logger.info("result.size:" + result.size());
                    //panda:{project}:ancPop:{qid}:map
                    Set<String> keys = new HashSet<>();
                    Pipeline pipelined = jedis.pipelined();
                    for (Tuple3<String, String, String> tuple3 : result) {
                        pipelined.evalsha(shaBroadcast.getValue(), 1, tuple3._1(), tuple3._2(), tuple3._3());
                        logger.info("pipelined.evalsha,key=" + tuple3._1() + ";value=" + tuple3._2() + " " + tuple3._3());
                        //panda:{project}:ancPop:{qid}:map:{day}
                        keys.add(new StringBuffer(tuple3._1()).append(":").append(tuple3._2()).toString());
                    }
                    pipelined.sync();
                    List<Tuple3<String, Long, String>> rankTuples = new ArrayList<>();
                    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");
                    DateTimeFormatter monthFor = DateTimeFormat.forPattern("yyyyMM");
                    logger.info("keys.size:" + keys.size());
                    for (String key : keys) {
                        //panda:{project}:ancPop:{qid}:map:{day}
                        String[] split = key.split(":");
                        String project = split[1];
                        String qid = split[3];
                        String day = split[5];
                        RankProject rankProject = rankProjectMap.get(project);
                        Map<String, String> qidDayPop = jedis.hgetAll(key.substring(0, key.lastIndexOf(":")));
                        DateTime curDateTime = formatter.parseDateTime(day);//当前日志日期
                        int daySize = (int) ((curDateTime.getMillis() - rankProject.getStartTimeU() * 1000l) / 86400000l) + 1;//距离活动开始日期的天数
                        logger.info("key " + key.substring(0, key.lastIndexOf(":")) + "; day:" + day + "; project:" + project + "; qidDayPop:" + qidDayPop + "; daySize:" + daySize);
                        if (rankProject.isPopularRank()) {//人气总榜
                            addRankTuple(split, qid, daySize, "ancPop", qidDayPop, formatter, curDateTime, rankTuples);
                        }
                        if (rankProject.isWeekPopularRank()) {//人气周榜 dividend被除数 divisor除数  dividend%divider
                            int days = 1;
                            int dayOfWeek = curDateTime.dayOfWeek().get();
                            days = dayOfWeek > daySize ? daySize : dayOfWeek;
                            int week = curDateTime.weekOfWeekyear().get();
                            addRankTuple(split, qid, days, "ancWkPop" + week, qidDayPop, formatter, curDateTime, rankTuples);
                        }
                        if (rankProject.isMonthPopularRank()) {//人气月榜
                            int days = 1;
                            int dayOfMonth = curDateTime.dayOfMonth().get();
                            days = dayOfMonth > daySize ? daySize : dayOfMonth;
                            String month = monthFor.print(curDateTime);
                            addRankTuple(split, qid, days, "ancMthPop" + month, qidDayPop, formatter, curDateTime, rankTuples);
                        }
                    }
                    logger.info("rankTuples.size:" + rankTuples.size());
                    Map<String, Set<String>> keyQidsMap = new HashMap<>();
                    Set<Tuple3<String, String, String>> newResult = new HashSet<>();
                    for (Tuple3<String, Long, String> tuple : rankTuples) {
                        pipelined.zadd(tuple._1(), tuple._2(), tuple._3());
                        logger.info("pipelined.zadd key=" + tuple._1() + "; value:" + tuple._2() + " " + tuple._2());
                        String[] split = tuple._1().split(":");
                        if (split.length != 4) {
                            continue;
                        }
                        Set<String> qids = null;
                        if (keyQidsMap.containsKey(tuple._1())) {
                            qids = keyQidsMap.get(tuple._1());
                        } else {
                            qids = new HashSet<>();
                            keyQidsMap.put(tuple._1(), qids);
                        }
                        qids.add(tuple._3());
                        String newKey = "";
                        if (rankProjectMap.get(split[1]).getFlag() == 1) {
                            newKey = new StringBuffer(split[0]).append(":").append(split[1]).append(":signUp:").append(split[2]).append(":").append(split[3]).toString();
                            newResult.add(new Tuple3<String, String, String>(tuple._1(), newKey, tuple._3()));
                            logger.info("newResult.add(new Tuple3<String, String, String>(" + tuple._1() + "," + newKey + "," + tuple._3() + ")");
                        }
                    }
                    pipelined.sync();
                    if (null != pipelined) {
                        pipelined.close();
                    }
                    List<Tuple3<String, Double, String>> singUpRecords = null;
                    for (Tuple3<String, String, String> tuple3 : newResult) {
                        String project = tuple3._1().split(":")[1];
                        if (jedis.sismember("hostpool:" + project, tuple3._3())) {
                            Double zscore = jedis.zscore(tuple3._1(), tuple3._3());
                            if (null == singUpRecords) {
                                singUpRecords = new ArrayList<>();
                            }
                            singUpRecords.add(new Tuple3<>(tuple3._2(), zscore, tuple3._3()));
                            logger.info("singUpRecords.add(new Tuple3<>(" + tuple3._2() + "," + zscore + "," + tuple3._3() + ")");
                        }
                    }
                    if (null != singUpRecords) {
                        pipelined = jedis.pipelined();
                        for (Tuple3<String, Double, String> tuple3 : singUpRecords) {
                            pipelined.zadd(tuple3._1(), tuple3._2(), tuple3._3());
                            logger.info("pipelined.zadd(" + tuple3._1() + "," + tuple3._2() + "," + tuple3._3());
                            Set<String> qids = null;
                            if (keyQidsMap.containsKey(tuple3._1())) {
                                qids = keyQidsMap.get(tuple3._1());
                            } else {
                                qids = new HashSet<>();
                                keyQidsMap.put(tuple3._1(), qids);
                            }
                            qids.add(tuple3._3());
                        }
                        pipelined.sync();
                        if (null != pipelined) {
                            pipelined.close();
                        }
                    }
                    try {
                        if (keyQidsMap.size() > 0) {
                            Set<String> needInfoQids = new HashSet<>();//需要更新缓存信息的
                            for (Map.Entry<String, Set<String>> entry : keyQidsMap.entrySet()) {
                                String key = entry.getKey();
                                Set<String> qids = entry.getValue();
                                Set<String> rankQids = jedis.zrevrange(key, 0, 100);
                                qids.retainAll(rankQids);
                                needInfoQids.addAll(qids);
                            }
                            if (needInfoQids.size() > 0) {
                                setBatchUserInfo(needInfoQids, jedis, mapper, qidRoomIdMap, true);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
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


    private static void setBatchUserInfo(Set<String> qids, Jedis jedis, ObjectMapper mapper, Map<String, String> qidRoomIdMap, boolean isAnchor) {
        Set<String> newRids = new HashSet<>();
        for (String qid : qids) {
            if (jedis.exists(new StringBuffer("panda:detail:usr:").append(qid).append(":info").toString())) {
                continue;
            }
            newRids.add(qid);
        }
        if (newRids.size() > 0) {
            try {
                String rids = newRids.stream().reduce((a, b) -> a + "," + b).get();
                String detailUrl = "http://u.pdtv.io:8360/profile/getavatarornickbyrids?rids=" + rids;
                String detailJson = httpGet(detailUrl);
                logger.info("json:" + detailJson);
                JsonNode detailJsonNode = mapper.readTree(detailJson);
                JsonNode dataNode = detailJsonNode.get("data");//data和子节点rid不为空，rid与rid不一致说明没有数据
                Pipeline pipelined = jedis.pipelined();
                for (String newRid : newRids) {
                    JsonNode detailNode = dataNode.get(newRid);
                    if (newRid.equalsIgnoreCase(detailNode.get("rid").asText())) {
                        Map<String, String> map = new HashedMap();
                        String detailKey = new StringBuffer("panda:detail:usr:").append(newRid).append(":info").toString();
                        map.put("rid", newRid);
                        map.put("nickName", detailNode.get("nickName").asText());
                        map.put("avatar", detailNode.get("avatar").asText());
                        if (qidRoomIdMap.containsKey(newRid) || isAnchor) {
                            map.put("roomId", qidRoomIdMap.get(newRid));
                            pipelined.set(detailKey, mapper.writeValueAsString(map));
                        } else {
                            map.put("roomId", "");
                            pipelined.setnx(detailKey, mapper.writeValueAsString(map));
                        }
                        pipelined.expire(detailKey, 86000 * 40);
                    }
                }
                pipelined.sync();
                pipelined.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static String httpGet(String url) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);

        //添加请求头
        request.addHeader("User-Agent", "Mozilla/5.0");

        HttpResponse response = client.execute(request);

        return EntityUtils.toString(response.getEntity(), "utf-8");
    }


    private static void addRankTuple(String[] split, String qid, int days, String rankFlag, Map<String, String> qidDayPop, DateTimeFormatter formatter, DateTime curDateTime, List<Tuple3<String, Long, String>> rankTuples) {
        long avgPop = 0;
        int dividend = 0;
        int divisor = 0;
        for (int i = 0; i < days; i++) {
            String date = formatter.print(curDateTime.plusDays(-i));
            String pop = qidDayPop.getOrDefault(date, "0");
            logger.info("rankFlag:" + rankFlag + "; pop==0;date:" + date + "; qidDayPop:" + qidDayPop + "; qidDayPop.get(" + date + ")" + qidDayPop.get(date) + "; i:" + i + "; days:" + days);
            if ("0".equals(pop)) {
                continue;
            }
            divisor++;
            dividend += Integer.parseInt(pop);
        }
        logger.info("dividend:" + dividend + ";divisor:" + divisor + "; rankFlag:" + rankFlag);
        if (divisor > 0 && dividend > 0) {
            avgPop = dividend / divisor;
            String rankKey = new StringBuffer(split[0]).append(":").append(split[1]).append(":").append(rankFlag).append(":rank").toString();
            rankTuples.add(new Tuple3<String, Long, String>(rankKey, avgPop, qid));
            logger.info("rankTuples.add key=" + rankKey + " avgPop=" + avgPop + " qid=" + qid);
        }
    }

    private static void executeSinglePojectPopular(Jedis jedis, Map.Entry<String, RankProject> entry, ShadowPopularity sp, List<Tuple3<String, String, String>> result, DateTimeFormatter parse, DateTimeFormatter dayFormat) {
        RankProject rankProject = entry.getValue();
        long startTimeU = rankProject.getStartTimeU();
        long endTimeU = rankProject.getEndTimeU();
        long timeU = sp.getTimestamp();
        String cate = sp.getCate();
        if (timeU < startTimeU || timeU > endTimeU) {
            return;
        }
        if (rankProject.getCates().size() > 0 && !rankProject.getCates().contains(cate)) {
            return;
        }
//        if (rankProject.getFlag() == 1 && !jedis.sismember("hostpool:" + rankProject.getProject(), sp.getHostId())) {//报名或者提供主播列表方式
//            return;
//        }
        String day = "";
        String week = "";
        try {
            DateTime dateTime = parse.parseDateTime(sp.getTime());
            day = dayFormat.print(dateTime);
            week = String.valueOf(dateTime.weekOfWeekyear().get());
        } catch (Exception e) {
            e.printStackTrace();
        }
        //panda:{project}:ancPop:{qid}:map
        String mapKey = new StringBuffer("panda:").append(rankProject.getProject()).append(":ancPop:").append(sp.getHostId()).append(":map").toString();
        result.add(new Tuple3<>(mapKey, day, String.valueOf(sp.getNum())));
    }

    private static ShadowPopularity getShadowPopularity(String next, ObjectMapper mapper) {
        ShadowPopularity sp = new ShadowPopularity();
        try {
            JsonNode jsonNode = mapper.readTree(next);
            String dataStr = jsonNode.get("data").asText();
            JsonNode data = mapper.readTree(dataStr);
            sp.setCate(data.get("classification").asText());
            sp.setHostId(data.get("hostid").asText());
            sp.setRoomId(data.get("roomid").asText());
            sp.setNum(data.get("person_num").asInt());
            sp.setTimestamp(data.get("timestamp").asLong());
            sp.setTime(jsonNode.get("time").asText());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("next:" + next);
        }
        return sp;
    }

    /**
     * 人气只统计flag=0或1的情况，分组不统计
     *
     * @return
     */
    private static Map<String, RankProject> getProjectMap() {
        Jedis jedis = new Jedis(redisHost, redisPort);
        if (StringUtils.isNotEmpty(redisPwd)) {
            jedis.auth(redisPwd);
        }
        Map<String, RankProject> projectsMap = new HashMap<>();
        Map<String, String> projectMap = jedis.hgetAll(projectKeyBroadcast.getValue());
        for (Map.Entry<String, String> entry : projectMap.entrySet()) {
            try {
                String key = entry.getKey();
                String value = entry.getValue();
                Map<String, String> paramMap = Splitter.on(",").withKeyValueSeparator("=").split(value);
                if (!paramMap.containsKey("project") || !paramMap.containsKey("startTimeU") || !paramMap.containsKey("endTimeU") || !paramMap.containsKey("flag") || (!paramMap.containsKey("popularRank") && !paramMap.containsKey("weekPopularRank") && !paramMap.containsKey("monthPopularRank"))) {
                    logger.info("没有人气榜单需求，key:" + key + ";value:" + value);
                    continue;
                }
                if (!Boolean.parseBoolean(paramMap.getOrDefault("online", "true"))) {
                    continue;
                }
                int flag = Integer.parseInt(paramMap.get("flag"));
                if (flag != 0 && flag != 1) {
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
                rankProject.setWeekPopularRank(Boolean.parseBoolean(paramMap.get("weekPopularRank")));
                rankProject.setMonthPopularRank(Boolean.parseBoolean(paramMap.get("monthPopularRank")));
                rankProject.setFlag(flag);
                rankProject.setUserLevel(Boolean.parseBoolean(paramMap.get("userLevel")));
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
        if (map.containsKey("bootServers")) {
            bootServers = map.get("bootServers");
        }
        if (map.containsKey("redisHost")) {
            redisHost = map.get("redisHost");
        }
        if (map.containsKey("redisPwd")) {
            redisPwd = map.get("redisPwd");
        }
        if (map.containsKey("redisPort")) {
            redisPort = Integer.parseInt(map.get("redisPort"));
        }
        if (map.containsKey("maxRatePerPartition")) {
            maxRatePerPartition = map.getOrDefault("maxRatePerPartition", "500");
        }
        name = map.getOrDefault("name", "popular_rank");
        logger.info("groupId:" + groupId + ";maxRatePerPartition:" + maxRatePerPartition);
    }
}
