package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 0) iter
        else Nil.iterator
      }
    )

mapRDD.collect().foreach(println)
    sc.stop()
  }
}
