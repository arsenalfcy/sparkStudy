package com.fu.bigdata.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object flatMapTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("flatmap")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("Hello scala", "Hello spark", "Hello hive"))


    val mapRDD = rdd.map(_.split(" ").mkString(",,"))
    mapRDD.collect().foreach(println)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    println("-------------")
    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
