package com.pandatv;

import org.junit.Test;
import redis.clients.jedis.Jedis;

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
        Long hello = jedis.sadd("sadd:test", "hello");
        System.out.println(hello);
        String[] split = "a:b:c:d:123:20180810".split(":");
    }
}
