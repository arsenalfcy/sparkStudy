package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object P54_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/apache.log")
    val mapHour = rdd.map(
      line => {
        val datas = line.split(" ")
        val hour = datas(3).takeRight(8).take(2)
        (hour,1)
      }
    )

    val result = mapHour.reduceByKey(_ + _).sortBy(_._1)
    result.collect().foreach(println)
  }
}
