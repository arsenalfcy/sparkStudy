package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object PartitionTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("par").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.defaultMinPartitions
    val rdd = sc.makeRDD(List("a", "b", "c", "d"))

    val partitions = rdd.getNumPartitions
    println(partitions)

    sc.stop()
  }
}
