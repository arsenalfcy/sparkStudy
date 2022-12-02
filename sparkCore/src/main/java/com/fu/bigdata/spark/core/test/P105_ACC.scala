package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object P105_ACC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val sumAcc = sc.longAccumulator("sum")
    rdd.foreach(
      num=>sumAcc.add(num)
    )

    println(sumAcc.sum)

    sc.stop()
  }
}
