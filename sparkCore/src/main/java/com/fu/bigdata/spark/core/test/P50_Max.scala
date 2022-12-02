package com.fu.bigdata.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object P50_Max {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    val glomRDD = rdd.glom()
    val maxInPartion: RDD[Int] = glomRDD.map(_.max)
    maxInPartion.collect().foreach(println)

    val sum = maxInPartion.sum().toInt
    println(sum)
    sc.stop()
  }
}
