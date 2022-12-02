package com.fu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaing03_DIY {
  def main(args: Array[String]): Unit = {

    // TODO: 创建环境对象
    //streamingContext创建时需要传递两个参数
    //第一个参数，环境配置
    //第二个参数，采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val messageDS = ssc.receiverStream(new MyReceiver)
    messageDS.print()

    //启动采集器
    ssc.start()




    //等待采集器关闭
    ssc.awaitTermination()

  }
  class MyReceiver extends Receiver [String](StorageLevel.MEMORY_ONLY){
    private var flag = true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag) {
            val message = "采集的数据为："+new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }

}
