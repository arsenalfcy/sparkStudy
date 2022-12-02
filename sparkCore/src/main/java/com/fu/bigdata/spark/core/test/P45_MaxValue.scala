package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object P45_MaxValue {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    val result = rdd.mapPartitions(
      datas => {
        val list = List(datas.max)
        list.toIterator
      }
    )

    result.collect().foreach(println)

    sc.stop()
  }
}
