package com.fu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val i = rdd.reduce(_ + _)
    println(i)

    val ints = rdd.collect()
    println(ints.mkString(","))

    val count = rdd.count()
    println(count)

    sc.stop()
  }
}
