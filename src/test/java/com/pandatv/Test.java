package com.pandatv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pandatv.bean.RankProject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import redis.clients.jedis.*;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: likaiqing
 * @create: 2018-11-02 17:06
 **/
public class Test {
    @org.junit.Test
    public void test1() throws IOException {
        Jedis jedis = new Jedis("localhost", 6379);
        String str = jedis.get("panda:zhaomu:ancdtl:27072326");
        ObjectMapper mapper = new ObjectMapper();
        HashMap hashMap = mapper.readValue(str, HashMap.class);
        System.out.println(hashMap);
        GeoRadiusParam radiusParam = GeoRadiusParam.geoRadiusParam().withCoord().withDist().sortAscending();
        List<GeoRadiusResponse> geoRadiusResponses = jedis.georadiusByMember("geo_test", "company", 20.0, GeoUnit.KM, radiusParam);

        jedis.close();
    }

    @org.junit.Test
    public void test2() throws IOException {
        String str = "{\"name\":\"ruc_profile_change\",\"data\":\"{\\\"rid\\\":154764538,\\\"nickname\\\":\\\"\\\\u7530\\\\u679c\\\\u679c\\\\u7684\\\\u7238\\\\u7238\\\"}\",\"host\":\"ruc7v.main.bjtb.pdtv.it\",\"key\":\"\",\"time\":\"2018-10-30 00:01:21\",\"requestid\":\"1540828881829-56913101-26916-f2dea295864b0486\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(str);
        String data = jsonNode.get("data").asText();
        String rid = mapper.readTree(data).get("rid").asText();
        System.out.println(rid);


    }


    @org.junit.Test
    public void test5() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String next = "{\"data\":\"{\\\"classification\\\":\\\"starface\\\",\\\"hostid\\\":\\\"82436604\\\",\\\"person_num\\\":184,\\\"roomid\\\":\\\"4235606\\\",\\\"timestamp\\\":1542618451}\",\"host\":\"pt9v.plat.bjtb.pdtv.it\",\"key\":\"\",\"name\":\"shadow_show_person_num\",\"requestid\":\"\",\"time\":\"2018-11-19 17:07:31\"}";
        JsonNode jsonNode = mapper.readTree(next);
        JsonNode dataNode = jsonNode.get("data");
        String hostid = mapper.readTree(dataNode.asText()).get("hostid").asText();
        System.out.println(jsonNode);
    }

    @org.junit.Test
    public void test3() throws IOException, InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(100);
        List<Object> list = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
            list.add(service.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    Jedis jedis = new Jedis("localhost", 6379);
                    Pipeline pipelined = jedis.pipelined();
                    for (int i = 0; i < 10000; i++) {
                        pipelined.zincrby("zincrby_test", 1, "test6");
                    }
                    pipelined.sync();
                    try {
                        pipelined.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    jedis.close();
                    return true;
                }
            }));
        }
        service.shutdown();
        boolean b = service.awaitTermination(20, TimeUnit.SECONDS);
        if (b) {
            System.out.println(b);
        }
        System.out.println(list.size());
        Jedis jedis = new Jedis("localhost", 6379);
        Set<Tuple> tuples = jedis.zrangeWithScores("zincrby_test", 0, 0);
        for (Tuple tuple : tuples) {
            System.out.println(tuple.getElement() + ":" + tuple.getScore());
        }
        jedis.close();

    }

    @org.junit.Test
    public void test6() throws IOException, InterruptedException {
        HashMap<String, Integer> map = new HashMap<>();
        Integer res = map.values().stream().reduce((a, b) -> a + b).orElse(0);
        System.out.println(res);
    }

    @org.junit.Test
    public void test7() throws IOException, InterruptedException {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");
        DateTime curDateTime = formatter.parseDateTime("20181129");//当前日志日期
        int daySize = (int) ((curDateTime.getMillis() - (1543075200 * 1000l)) / 86400000l) + 1;
        System.out.println(daySize);
    }

    @org.junit.Test
    public void test8() throws IOException, InterruptedException {
        RankProject rankProject = new RankProject();
        List<String> giftIds = new ArrayList<>();
        rankProject.setGiftIds(giftIds);

    }

    @org.junit.Test
    public void test9() throws IOException, InterruptedException {
        Set<Tuple3<String, String, String>> sets = new HashSet<>();
        Tuple3<String, String, String> tuple3 = new Tuple3<>("1", "1", "1");
        Tuple3<String, String, String> tuple3_2 = new Tuple3<>("1", "1", "1");
        sets.add(tuple3);
        sets.add(tuple3_2);
        System.out.println(sets.size());
    }

    @org.junit.Test
    public void test10() throws IOException, InterruptedException {
        Jedis jedis = new Jedis("localhost", 6379);
        Set<String> set = new HashSet<>();
        set.add("test1");
        set.add("test2");
        set.add("test3");
        set.add("test4");
        String[] keys = set.stream().toArray(String[]::new);
        List<String> mget = jedis.mget(keys);
        for (String s : mget) {
            System.out.println(s);
        }
        jedis.close();
    }

    @org.junit.Test
    public void test11() throws IOException, InterruptedException, ParseException {
        boolean contains = "abc".contains("");

        String substring = "1544630316:c".substring(11);
        BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/likaiqing/Downloads/client_online_147819952.log")));
        String l = null;
        Set<Tuple2<String, String>> list = new HashSet<>();
        while ((l = bf.readLine()) != null) {
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
            String flag = "p";
            if (l.contains("GET /client_punch.gif?")) {
                flag = "c";
//                int roomIdIndex = l.indexOf("rid=");
//                int roomIdIndex1 = l.indexOf("&", roomIdIndex);
//                int roomIdIndex2 = l.indexOf(" ", roomIdIndex);
//                int roomIdEndIndex = 0;
//                if (roomIdIndex1 < 0) {
//                    roomIdEndIndex = roomIdIndex2;
//                } else {
//                    roomIdEndIndex = roomIdIndex1;
//                }
//                String roomId = l.substring(roomIdIndex + 4, roomIdEndIndex);
//                System.out.println(roomId + "," + uid + "-" + timeU + ":" + flag);
//                list.add(new Tuple2<String, String>(roomId, uid + "-" + timeU + ":" + flag));
//                continue;
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
            list.add(new Tuple2<String, String>(stream, uid + "-" + timeU + ":" + flag));
            System.out.println(stream);
        }
        System.out.println(list.size());
        System.out.println(list);
        ReentrantLock lock = new ReentrantLock();
    }

    @org.junit.Test
    public void test12() {
        DateTime dateTime = new DateTime();
        int week = dateTime.weekOfWeekyear().get();
        System.out.println(week);
    }

    @org.junit.Test
    public void test13() throws IOException, InterruptedException {
        Set<String> newQids = new HashSet<>();
        newQids.add("23175476");
        newQids.add("23233766");
        newQids.add("92349566");
        ObjectMapper mapper = new ObjectMapper();
        String json = getBatchUserInfo(newQids.stream().reduce((a, b) -> a + "," + b).get());
        JsonNode jsonNode = mapper.readTree(json);
        JsonNode dataNode = jsonNode.get("data");
        String s = jsonNode.get("data").get("23175476").get("level").asText();
        if (null != dataNode) {
            JsonNode jsonNode1 = dataNode.get("23175476");
            JsonNode jsonNode2 = dataNode.get("23233766");
        }
    }

    private static String getBatchUserInfo(String rids) throws IOException {
//        String url = "http://u.pdtv.io:8360/profile/getavatarornickbyrids?rids=" + rids;
//        String url = "http://count.pdtv.io:8360/number/pcgame_pandatv/user_exp/list?rids=" + rids;
        String url = "http://count.pdtv.io:8360/number/pcgame_pandatv/user_exp/list?rids=123,12323,43243";
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);

        //添加请求头
        request.addHeader("User-Agent", "Mozilla/5.0");

        HttpResponse response = client.execute(request);

        return EntityUtils.toString(response.getEntity(), "utf-8");
    }

    @org.junit.Test
    public void test18() throws IOException {
        String url = "http://count.pdtv.io:8360/number/pcgame_pandatv/user_exp/list?rids=123,12323,43243";
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);

        //添加请求头
        request.addHeader("User-Agent", "Mozilla/5.0");

        HttpResponse response = client.execute(request);

        String result = EntityUtils.toString(response.getEntity(), "utf-8");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode levelJsonNode = mapper.readTree(result);
        String l = levelJsonNode.get("data").get("123").get("level").asText();
        System.out.println(l);

    }

    @org.junit.Test
    public void test14() throws IOException, InterruptedException {
        Set<String> set1 = new HashSet<>();
        set1.add("a1");
        set1.add("b");
        set1.add("c1");
        Set<String> set2 = new HashSet<>();
        set2.add("a");
        set2.add("d");
        set2.add("c");
        set2.add("e");
        boolean b = set1.retainAll(set2);
        System.out.println(set1);
    }

    @org.junit.Test
    public void test15() throws IOException, InterruptedException {

        Math.abs(-0.23);
        RankProject rankProject = new RankProject();
        Map<String, String> map = new HashMap<>();
        rankProject.setWeekPopularRank(Boolean.parseBoolean(map.get("weekPopularRank")));
        System.out.println(rankProject);
    }
}
