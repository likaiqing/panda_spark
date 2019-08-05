package com.pandatv.scala.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

object UserClickCountAnalytics {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val conf = new SparkConf().setAppName("UserClickCountAnalytics").setMaster(masterUrl)
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("file:///Users/likaiqing/Downloads/checkpoint1")
    val topic = Set("user_event")
    val brokers = "10.131.6.79:9092"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> "test",
      //      "auto.offset.reset" -> "latest",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaParams))

    val userClicks = kafkaStream.map(line => {
      val jsonValue = JSON.parseFull(line.value()).get
      val jsonObject = jsonValue.asInstanceOf[Map[String, Any]]
      (jsonObject("uid"), jsonObject("click_count").asInstanceOf[Double].toInt)
    })
    //      .reduceByKey(_ + _)

    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        par.foreach(pair => {
          val uid = pair._1
          val clickCount = pair._2
          val jedis = RedisClient.pool.getResource
          jedis.hincrBy("user_click_test", uid.toString, clickCount.toLong)
          RedisClient.pool.returnBrokenResource(jedis)
        })
      })
    })

    userClicks.updateStateByKey[Int](updateFunction _).foreachRDD(rdd => {
      rdd.foreachPartition(par => {
        par.foreach(println)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    var newCount = 0
    if (runningCount.nonEmpty) {
      newCount = runningCount.get
    }
    var res = Option(newCount)
    if (!newValues.isEmpty) {
      val max = newValues.max[Int]
      if (max > newCount) {
        res = Option(max)
      }
    }
    res
  }

}
