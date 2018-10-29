package com.pandatv;

import com.pandatv.tools.RedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "t10v.infra.bjtb.pdtv.it:9092");
//        props.put("acks", "all");
////        props.put("retries", 0);
////        props.put("transactional.id", transactionalId);
//        props.put("batch.size", 5);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 3554432);
//        props.put("enable.idempotence", true);//幂等性，retries不再设置，默认int最大值
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        Producer<String, String> producer = new KafkaProducer<>(props);
//        try {
//            for (int i = 0; i < 10; i++) {
//                RecordMetadata metadata = producer.send(new ProducerRecord<String, String>("pcgameq_panda_watch_time_sign", "123435-5")).get();
//                String res = metadata.topic() + ";partition:" + metadata.partition() + ";offset:" + metadata.offset();
//                System.out.println(res);
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//
//        producer.close();

//        Jedis jedis = new Jedis("localhost", 6379);
//        String res = jedis.set("set_test", "1", "NX", "PX", 30000);
//        Transaction multi = jedis.multi();
//        jedis.hincrBy("map_test", "a", 10);
//        jedis.hincrBy("map_test", "b", 10);
//        RedisClient client = new RedisClient("localhost", 6379);
        ExecutorService service = Executors.newFixedThreadPool(400);
//        for (int i = 0; i < 400; i++) {
//            service.submit(new Runnable() {
//                @Override
//                public void run() {
//                    Jedis jedis = client.getResource();
//                    long threadId = Thread.currentThread().getId();
//                    while (!jedis.set("map_test_lock", String.valueOf(threadId), "NX", "PX", 1000).equalsIgnoreCase("OK")) {
//                        try {
//                            Thread.sleep(10);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                    System.out.println(threadId);
//                    Transaction multi = jedis.multi();
//                    multi.hincrBy("map_test", "test9", 1);
////                    multi.hincrBy("map_test", "test6", 1);
////                    multi.hincrBy("map_test", "test7", 1);
//                    multi.exec();
//                    if (jedis.get("map_test_lock").equalsIgnoreCase(String.valueOf(threadId))) {
//                        System.out.println(threadId + ":del");
//                        jedis.del("map_test_lock");
//                    }
//                    client.returnResource(jedis);
//                }
//            });
//        }
//        for (int i = 0; i < 60; i++) {
//            service.submit(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        RedisClient client = new RedisClient("localhost", 6379);
//                        for (int j = 0; j < 100; j++) {
//                            Jedis jedis = client.getResource();
//                            Transaction multi = jedis.multi();
//                            jedis.hincrBy("map_test", "1", 1);
//                            multi.exec();
//                            client.returnResource(jedis);
//                        }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }
        service.shutdown();
        RedisClient client = new RedisClient("localhost", 6379);
        Jedis jedis = client.getResource();
        Transaction multi = jedis.multi();
        for (int i = 0; i < 100; i++) {
            multi.hincrBy("map_test", "1", 1);
        }
        multi.exec();
        client.returnResource(jedis);
        System.out.println("结束");
    }
}
