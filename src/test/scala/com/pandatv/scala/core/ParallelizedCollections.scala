package com.pandatv.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizedCollections {

  //  def test1(): Unit = {
  //  }
  def test1(): Unit = {
    val data = Array(1, 2, 3, 4, 5)
    val conf = new SparkConf()
    conf.setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val distData = sc.parallelize(data)
    val sum = distData.reduce(_ + _)
    println(sum)
    sc.textFile("");
  }

  def test2(): String = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    val data = sc.textFile("file:///Users/likaiqing/Downloads/test.sh")
    data.count().toString

  }

  def main(args: Array[String]): Unit = {
    //    test1()
    val res = test2()
    println(res)
  }
}

class ParallelizedCollections {

}
