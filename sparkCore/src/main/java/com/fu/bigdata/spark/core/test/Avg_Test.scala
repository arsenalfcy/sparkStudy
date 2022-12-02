package com.fu.bigdata.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Avg_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Avg")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 5), ("b", 45), ("a", 22), ("b", 12)))

    val value: RDD[(String, (Int, Int))] = rdd.mapValues((_, 1))
    val value1 = value.reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    })

    value1.mapValues((data=>{(data._1,data._1/data._2)})).collect().foreach(println)

//    value1.collect().foreach(println)
  }
}
