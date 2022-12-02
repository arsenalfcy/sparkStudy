package com.fu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello scala", "Hello spark", "Hello Hadoop")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))
    val result: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    result.collect().foreach(println)

    println("-------------------")
    val list1 = List("Hello scala", "Hello spark", "Hello Hadoop")
    val rdd1 = sc.makeRDD(list)
    val flatRDD1 = rdd.flatMap(_.split(" "))
    val mapRDD1 = flatRDD.map((_, 1))
    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
