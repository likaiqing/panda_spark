package com.pandatv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() throws IOException {
        Jedis jedis = new Jedis("localhost", 6379);
        Long sadd = jedis.sadd("sadd_test", "test");
//        if (sadd>0){
//            Long sadd_test = jedis.scard("sadd_test");
//            System.out.println(sadd_test);
//        }
//        List<String> mget = jedis.mget(new String[]{});
//        System.out.println(mget);
//        Long hello = jedis.sadd("sadd:test", "hello");
//        System.out.println(hello);
//        String[] split = "a:b:c:d:123:20180810".split(":");
//        Map<String,String> streamMap = new HashMap<>();
//        List<String> streams = Lists.newArrayList(streamMap.keySet());
//        List<String> streamKeys = streams.stream().map(s -> new StringBuffer("stream:").append(s).toString()).collect(Collectors.toList());

//        List<String> roomIds = jedis.mget(streamKeys.toArray(new String[streamKeys.size()]));

//        SimpleDateFormat dayFormat = new SimpleDateFormat("yyyyMMdd");
//        String day = dayFormat.format(new Date(1539827913 * 1000l));
//        System.out.println(day);
//        String[] split = "uwdt:user:dur:pf:6442894:20181018".split(":");
//        String res = split[split.length - 2];
//        System.out.println(res);
        ObjectMapper mapper = new ObjectMapper();
        String str = "{\"name\":\"pcgameq_panda_gift_donate\",\"data\":\"{\\\"__plat\\\":\\\"ios\\\",\\\"uid\\\":\\\"146938196\\\",\\\"anchor\\\":\\\"50782598\\\",\\\"roomid\\\":\\\"594582\\\",\\\"giftid\\\":\\\"5aba0ed6ea3d187b391b2293\\\",\\\"price\\\":\\\"50\\\",\\\"count\\\":\\\"1\\\",\\\"total\\\":\\\"50\\\",\\\"ip\\\":\\\"10.31.131.23\\\",\\\"time\\\":1540796914.9594,\\\"channel\\\":\\\"\\\",\\\"unique\\\":\\\"5bd6b1eedcf63a32e75ad7e1_3\\\",\\\"pdft\\\":\\\"20161027143107341873a587566c989cf06ef1bb0c7a4cb2a02c43f678809f\\\",\\\"cate\\\":\\\"dnf\\\",\\\"fb\\\":\\\"\\\",\\\"lotteryChance\\\":\\\"\\\"}\",\"host\":\"pt6v.plat.bjtb.pdtv.it\",\"key\":\"\",\"time\":\"2018-10-29 15:08:34\",\"requestid\":\"1540796914777-81713101-23008-71e9cfb8d4b28bb2\"}\n";
        JsonNode node = mapper.readTree(str);
        String dataStr = node.get("data").asText();
        JsonNode data = mapper.readTree(dataStr);
        JsonNode uid = data.get("uid");
//        System.out.println(uid.toString());

        String json = sendGet("http://count.pdtv.io:8360/number/pcgame_pandatv/user_exp/get?rid=39946732");
        int level = mapper.readTree(json).get("data").get("level").asInt();
        System.out.println(level);
    }

    public static String sendGet(String url) {
        String result = "";
        BufferedReader in = null;
        try {
            String urlNameString = url;
            URL realUrl = new URL(urlNameString);
            URLConnection connection = realUrl.openConnection();
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.connect();
            Map<String, List<String>> map = connection.getHeaderFields();
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {

//            e.printStackTrace();
            return null;
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }
}
