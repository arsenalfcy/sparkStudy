package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object aggregateTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6))

  }
}
