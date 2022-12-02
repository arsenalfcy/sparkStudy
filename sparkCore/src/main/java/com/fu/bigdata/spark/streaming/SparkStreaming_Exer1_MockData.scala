package com.fu.bigdata.spark.streaming

import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkStreaming_Exer1_MockData {
  def main(args: Array[String]): Unit = {


    //生成模拟数据
  while (true) {
    mockData().foreach(
      data =>{
        //向kafka生成数据

      }
    )
    Thread.sleep(2000)
  }
  }
  def mockData()={
    val list =ListBuffer[String]()
    val areaList = ListBuffer[String]("华北","华东","华南")
    val cityList = ListBuffer[String]("北京","上海","深圳")
    for(i <- 1 to 30){
      val area = areaList(new Random().nextInt(3))
      val city = cityList(new Random().nextInt(3))
      var userId = new Random().nextInt(6)
      var adId = new Random().nextInt(6)
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userId} ${adId}")
    }
    list
  }
}
