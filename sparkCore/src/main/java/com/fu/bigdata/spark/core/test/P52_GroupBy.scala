package com.fu.bigdata.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object P52_GroupBy {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4,5,6,7,8,9,10,13,15,155),3)

    val result = rdd.groupBy(_ % 2 )

    rdd.saveAsTextFile("outef")

    result.collect().foreach(println)
    result.saveAsTextFile("output")
    sc.stop()
  }
}
