package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object groupByTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello", "hadoop", "hive", "spark", "scala", "flinke"))

    rdd.groupBy(word=>word.head=='h').saveAsTextFile("output")
    sc.stop()

  }
}
