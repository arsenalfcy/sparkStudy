package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd = sc.textFile("datas/apache.log")

    rdd.filter(
      data => {
        val datas = data.split(" ")
        val time = datas(3)
        time.startsWith("17/05/2015")
      }).collect().foreach(println)
    sc.stop()
  }
}
