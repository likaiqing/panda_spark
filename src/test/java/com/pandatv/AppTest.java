package com.pandatv;

import com.clearspring.analytics.util.Lists;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        Jedis jedis = new Jedis("localhost", 6379);
//        List<String> mget = jedis.mget(new String[]{});
//        System.out.println(mget);
//        Long hello = jedis.sadd("sadd:test", "hello");
//        System.out.println(hello);
//        String[] split = "a:b:c:d:123:20180810".split(":");
        Map<String,String> streamMap = new HashMap<>();
        List<String> streams = Lists.newArrayList(streamMap.keySet());
        List<String> streamKeys = streams.stream().map(s -> new StringBuffer("stream:").append(s).toString()).collect(Collectors.toList());

//        List<String> roomIds = jedis.mget(streamKeys.toArray(new String[streamKeys.size()]));
    }
}
