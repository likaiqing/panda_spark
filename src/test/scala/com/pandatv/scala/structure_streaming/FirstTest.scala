package com.pandatv.scala.structure_streaming

import org.apache.spark.sql.SparkSession

object FirstTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("FirstTest").getOrCreate()
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    lines.printSchema()
    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))
    words.printSchema()
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

  }

}
