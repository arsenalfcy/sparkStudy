package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object P42_apacheLog {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/apache.log")

    val mapRDD = rdd.map {
      datas => {
        val data = datas.split(" ")
        data(6)
      }
    }

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
