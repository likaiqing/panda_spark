package com.pandatv.scala.core.df

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}


object UserDefinedTypedAggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("test").getOrCreate()
    import spark.implicits._
    val ds = spark.read.json("/usr/local/Cellar/apache-spark/2.3.1/libexec/examples/src/main/resources/employees.json").as[Employee]
    ds.show()
    val averageSalary = MyAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()
    spark.stop()
  }

  case class Employee(name: String, salary: Long)

  case class Average(var sum: Long, var count: Long)

  object MyAverage extends Aggregator[Employee, Average, Double] {
    override def zero: Average = Average(0l, 0l)

    override def reduce(b: Average, a: Employee): Average = {
      b.sum += a.salary
      b.count += 1
      b
    }

    override def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Average): Double = {
      reduction.sum.toDouble / reduction.count
    }

    override def bufferEncoder: Encoder[Average] = {
      Encoders.product
    }

    override def outputEncoder: Encoder[Double] = {
      Encoders.scalaDouble
    }
  }

}
