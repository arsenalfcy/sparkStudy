package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4),1)
    val mapRDD = rdd.map(
      num => {
        println("num=" + num)
        num
      }
    )

    val mapRDD1 = mapRDD.map(
      num => {
        println("***num****=" + num)
        num
      }
    )

    mapRDD1.collect()


    sc.stop()
  }
}
