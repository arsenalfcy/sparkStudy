package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd = sc.makeRDD(List(1,2,3,4,2,4,5,7,8,5,6,7,8,9,10))

    val value = rdd.distinct()
    value.collect().foreach(println)




    sc.stop()
  }
}
