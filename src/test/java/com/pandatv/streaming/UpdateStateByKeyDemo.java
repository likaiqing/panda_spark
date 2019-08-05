package com.pandatv.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * @author: likaiqing
 * @create: 2019-02-22 10:54
 **/
public class UpdateStateByKeyDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyDemo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.checkpoint("file:///Users/likaiqing/Downloads/checkpoint");
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(a -> {
            return Arrays.asList(a.split(" ")).iterator();
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> wordsCount = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {//对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                //第一个参数就是key传进来的数据，第二个参数是曾经已有的数据
                Integer updatedValue = 0;//如果第一次，state没有，updatedValue为0，如果有，就获取
                if (state.isPresent()) {
                    updatedValue = state.get();
                }
                for (Integer value : values) {
                    updatedValue += value;
                }
                return Optional.of(updatedValue);//返回更新的值
            }
        });
        wordsCount.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
        new File("");
    }

}
