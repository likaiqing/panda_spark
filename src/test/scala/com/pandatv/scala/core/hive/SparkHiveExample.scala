package com.pandatv.scala.core.hive

import org.apache.spark.sql.SparkSession

object SparkHiveExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("spark hive example").enableHiveSupport().getOrCreate()
    import spark.implicits._
    import spark.sql
    sql("create table if not exists src (key INT, value STRING) USING hive")
    sql("load data local inpath '/usr/local/Cellar/apache-spark/2.3.1/libexec/examples/src/main/resources/kv1.txt' into table src")
    sql("select * from src").show()
  }

}
