package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    rdd.cache()
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val maxRDD = glomRDD.map(
      array => {
        array.max
      }
    )
    println(maxRDD.collect().sum)




    sc.stop()
  }
}
