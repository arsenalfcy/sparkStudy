package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object P61_DoubleValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RP")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))

    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    val rdd5 = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
  }
}
