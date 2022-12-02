package com.fu.bigdata.spark.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Load {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("output1")

    println(rdd.collect().mkString(","))

    val rdd1 = sc.objectFile[(String,Int)]("output2")
    println(rdd1.collect().mkString(","))

    val rdd2 = sc.sequenceFile[String,Int]("output3")
    println(rdd2.collect().mkString(","))
    sc.stop()
  }
}
