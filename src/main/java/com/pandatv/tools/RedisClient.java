package com.pandatv.tools;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.Serializable;

/**
 * @author: likaiqing
 * @create: 2018-10-11 15:05
 **/
public class RedisClient implements Serializable {
    public static JedisPool jedisPool;
    public String host;
    public String pwd;
    private static GenericObjectPoolConfig config;

    static {
        config = new GenericObjectPoolConfig();
        config.setMaxTotal(500);//
        config.setMinIdle(1);
    }

    public RedisClient() {
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
    }

    public RedisClient(String host, int port) {
        this.host = host;
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
        if (null == jedisPool) {
            synchronized (RedisClient.class) {
                jedisPool = new JedisPool(config, host, port);
            }
        }
    }

    public RedisClient(String host, int port, String pwd) {
        this.host = host;
        this.pwd = pwd;
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
        if (null == jedisPool) {
            synchronized (RedisClient.class) {
                jedisPool = new JedisPool(config, host, port);
            }
        }
    }

    static class CleanWorkThread extends Thread {
        @Override
        public void run() {
            System.out.println("Destroy jedis pool");
            if (null != jedisPool) {
                jedisPool.destroy();
                jedisPool = null;
            }
        }
    }

    public Jedis getResource() {
        Jedis jedis = jedisPool.getResource();
        if (StringUtils.isNotEmpty(pwd)) {
            jedis.auth(pwd);
        }
        return jedis;
    }

    public void returnResource(Jedis jedis) {
        jedis.close();
    }
}
