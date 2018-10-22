package com.pandatv;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "t10v.infra.bjtb.pdtv.it:9092");
        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("transactional.id", transactionalId);
        props.put("batch.size", 5);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 3554432);
        props.put("enable.idempotence", true);//幂等性，retries不再设置，默认int最大值
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            for (int i = 0; i < 10; i++) {
                RecordMetadata metadata = producer.send(new ProducerRecord<String, String>("pcgameq_panda_watch_time_sign", "123435-5")).get();
                String res = metadata.topic() + ";partition:" + metadata.partition() + ";offset:" + metadata.offset();
                System.out.println(res);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
