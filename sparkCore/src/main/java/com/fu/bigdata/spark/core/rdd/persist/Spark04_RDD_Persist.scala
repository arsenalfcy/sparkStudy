package com.fu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")   //指定的checkpoint路径

    val list = List("Hello scala", "Hello spark", "Hello Hadoop")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map(word=>{
      println("map调用")
      (word,1)
    })
    mapRDD.cache()
    mapRDD.checkpoint()       //需要落盘，需要指定检查点保存路径
    val result: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    result.collect().foreach(println)

    println("-------------------")

    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
