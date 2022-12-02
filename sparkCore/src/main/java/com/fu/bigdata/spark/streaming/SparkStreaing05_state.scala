package com.fu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaing05_state {
  def main(args: Array[String]): Unit = {

    // TODO: 创建环境对象
    //streamingContext创建时需要传递两个参数
    //第一个参数，环境配置
    //第二个参数，采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    ssc.checkpoint("cp")


    val datas = ssc.socketTextStream("localhost", 9999)
    val wordToOne = datas.map((_, 1))

    val wordToCount: DStream[(String, Int)] = wordToOne.updateStateByKey(
      (seq: Seq[Int], opt: Option[Int]) => {
        val newCount = opt.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )

    wordToCount.print()

    //启动采集器
    ssc.start()

    //等待采集器关闭
    ssc.awaitTermination()

  }


}
