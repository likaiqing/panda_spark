package com.pandatv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;

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
    public void test3() throws IOException {
        Jedis jedis = new Jedis("localhost", 6379);

        jedis.close();

    }
}
