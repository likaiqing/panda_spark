package com.pandatv.scala.core.jdbc

import org.apache.spark.sql.SparkSession

object SparkJdbcTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("spark jdbc example").getOrCreate()
    spark.read.format("jdbc")
      .option("url","jdbc:mysql.jdbc.")
  }

}
