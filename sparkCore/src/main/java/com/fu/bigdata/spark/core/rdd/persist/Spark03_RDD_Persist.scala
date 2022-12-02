package com.fu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello scala", "Hello spark", "Hello Hadoop")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map(word=>{
      println("map调用")
      (word,1)
    })
    mapRDD.cache()    //设置了缓存，两次action算子都从缓存中获取数据，避免了重复调用
    val result: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    result.collect().foreach(println)

    println("-------------------")

    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
