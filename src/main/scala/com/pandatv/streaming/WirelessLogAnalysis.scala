package com.pandatv.streaming

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.Date

import com.pandatv.tools.MysqlPool
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object WirelessLogAnalysis {

  object BroadcastWrapper {

    @volatile private var instance: Broadcast[Map[String, List[String]]] = null
    private val map = mutable.LinkedHashMap[String, List[String]]()

    def getMysql(): Map[String, List[String]] = {
      //1.获取mysql连接池的一个连接
      val conn = MysqlPool.getConnection
      //2.查询新的数据
      val sql = "select aid_type,aids from cf_similarity"
      val ps = conn.prepareStatement(sql)
      val rs = ps.executeQuery()

      while (rs.next()) {
        val aid = rs.getString("aid_type")
        val aids = rs.getString("aids").split(",").toList
        map += (aid -> aids)
      }
      //3.连接池回收连接
      conn.close()
      map.toMap
    }

    def update(sc: SparkContext, blocking: Boolean = false): Unit = {
      if (instance != null)
        instance.unpersist(blocking)
      instance = sc.broadcast(getMysql())
    }

    def getInstance(sc: SparkContext): Broadcast[Map[String, List[String]]] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = sc.broadcast(getMysql)
          }
        }
      }
      instance
    }

    private def writeObject(out: ObjectOutputStream): Unit = {
      out.writeObject(instance)
    }

    private def readObject(in: ObjectInputStream): Unit = {
      instance = in.readObject().asInstanceOf[Broadcast[Map[String, List[String]]]]
    }
  }


  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)
    val conf = new SparkConf()
      .setAppName("wirelessLogAnalysis")

    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams: Map[String, Object] = Map(
      "metadata.broker.list" -> "",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "broadcast_test",
      "bootstrap.servers" -> "10.131.6.79:9092",
      "auto.offset.reset" -> "false")

    val message = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String]("".split(","), kafkaParams)) //新加分区，如果没有设置，将会消费不到
    //原始日志流打印

    message.foreachRDD {
      rdd => {
        // driver端运行，涉及操作：广播变量的初始化和更新
        // 可以自定义更新时间
        if (true) {
          BroadcastWrapper.update(rdd.sparkContext, true)
          println("广播变量更新成功： " + new Date())
        }
        BroadcastWrapper.getInstance(ssc.sparkContext).value
        //worker端运行，涉及操作：Dstream数据的处理和Redis更新
        //        rdd.foreachPartition {
        //          partitionRecords =>
        //            //1.获取redis连接，保证每个partition建立一次连接，避免每个记录建立/关闭连接的性能消耗
        //            partitionRecords.foreach(
        //              record => {
        //                //2.处理日志流
        //                println(record)
        //              }
        //              //3.redis更新数据
        //            )
        //          //4.关闭redis连接
        //        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
