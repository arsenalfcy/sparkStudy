package com.fu.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    // TODO: 建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO: 执行业务操作

    val lines = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    println("----------------")
    val words = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("----------------")
    val wordToOne = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println("----------------")
    val reduceRDD = wordToOne.reduceByKey(_ + _)
    println(reduceRDD.toDebugString)
    println("----------------")
    reduceRDD.collect().foreach(println)
    // TODO: 关闭连接
    sc.stop()

  }
}
