package com.pandatv.scala.streaming

import java.io.File

import com.google.common.io.Files
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

object UserClickCountAnalytics2 {

  def createContext(ip: String, port: Int, outputPath: Any, checkpoint: String): StreamingContext = {
    println("Creating new context")

    null
  }

  def main(args: Array[String]): Unit = {
    val checkpoint = "file:///Users/likaiqing/Downloads/checkpoint1"
    val ip = "localhost"
    val port = 9999
    val outputPath = "/Users/likaiqing/Downloads/file_test"
    val ssc = StreamingContext.getOrCreate(checkpoint,()=>createContext(ip,port,outputPath,checkpoint))
    ssc.start()
    ssc.awaitTermination()
  }

}
