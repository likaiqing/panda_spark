package com.pandatv.streaming

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Encoders, SparkSession}

object Main {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("kafka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()


    var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    import spark.implicits._
    lines.as(Encoders.STRING)
      .map(row => {
        val fields = row.trim.split(",")
        MyEntity(fields(0), new Timestamp(sdf.parse(fields(1)).getTime), Integer.valueOf(fields(2)))
      })
      .createOrReplaceTempView("tv_entity")
    spark.sql("select id,timestamp,value from tv_entity")
      .withWatermark("timestamp", "3 seconds")
      .createOrReplaceTempView("tv_entity_watermark")

    val resultDf = spark.sql(
      s"""
         |select id,min(timestamp) min_timestamp,max(timestamp) max_timestamp,sum(value) as sum_value
         |from  tv_entity_watermark
         |group by window(timestamp,'10 seconds','3 seconds'),id
         |""".stripMargin)

    val query = resultDf.writeStream.format("console").outputMode(OutputMode.Update()).start()

    query.awaitTermination()
    query.stop()
  }

  case class MyEntity(id: String, timestamp: Timestamp, value: Integer)

}
