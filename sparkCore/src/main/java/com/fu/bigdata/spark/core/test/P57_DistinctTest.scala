package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object P57_DistinctTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RP")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 23, 21, 12, 2, 3, 4, 3, 2, 3, 4, 54, 3, 4))
    val distinctRDD = rdd.distinct(2)

    distinctRDD.collect().foreach(println)

  }
}
