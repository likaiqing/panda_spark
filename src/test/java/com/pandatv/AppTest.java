package com.pandatv;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.Date;

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
        Long sadd = jedis.sadd("sadd_test", "test");
        if (sadd>0){
            Long sadd_test = jedis.scard("sadd_test");
            System.out.println(sadd_test);
        }
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
    }
}
