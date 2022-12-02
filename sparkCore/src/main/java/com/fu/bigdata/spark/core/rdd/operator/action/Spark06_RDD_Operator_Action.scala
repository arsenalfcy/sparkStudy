package com.fu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    rdd.collect().foreach(println)    //foreach是driver端内存集合的循环遍历方法
    println("**********")
    rdd.foreach(println)    //executor端内存数据打印



    sc.stop()
  }
}
