package com.pandatv.scala.core.df

import org.apache.spark.sql.{Encoders, SparkSession}

object SparkSQLExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    //    runBasicDataFrameExample(spark)
    inferringSchemaUseReflection(spark);
  }

  def inferringSchemaUseReflection(spark: SparkSession) = {
    import spark.implicits._
    val peopleDF = spark.sparkContext.textFile("/usr/local/Cellar/apache-spark/2.3.1/libexec/examples/src/main/resources/people.txt").map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF
    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("select name,age from people where age between 13 and 19")
    teenagersDF.map(teenager => "Name:" + teenager(0)).show()
    teenagersDF.map(teenager => "Name:" + teenager.getAs[String]("name")).show()
    implicit val mapEncoder = Encoders.kryo[Map[String, Any]]
    val map = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    map.foreach(m => {
      for ((k, v) <- m) {
        println("k:" + k + ";v:" + v)
      }
    })
    println(map)
  }

  case class Person(name: String, age: Long)

  def runBasicDataFrameExample(spark: SparkSession) = {
    val df = spark.read.json("/usr/local/Cellar/apache-spark/2.3.1/libexec/examples/src/main/resources/people.json")
    df.show()
    df.schema.fields.foreach(println(_))
    df.select("name").show()
    import spark.implicits._
    df.select($"name", $"age" + 1)
    df.filter($"age" > 21).show()
    df.groupBy("age").avg("age").show()
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()
  }

}
