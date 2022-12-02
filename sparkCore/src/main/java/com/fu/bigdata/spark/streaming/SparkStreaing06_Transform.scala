package com.fu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaing06_Transform {
  def main(args: Array[String]): Unit = {

    // TODO: 创建环境对象
    //streamingContext创建时需要传递两个参数
    //第一个参数，环境配置
    //第二个参数，采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val datas = ssc.socketTextStream("localhost", 9999)

    val value: DStream[String] = datas.transform(rdd => rdd)

    //启动采集器
    ssc.start()

    //等待采集器关闭
    ssc.awaitTermination()

  }


}
