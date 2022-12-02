package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 求平均值
    val rdd1 = sc.makeRDD(List(("a",1),("a",2),("a",3)))
    val rdd2 = sc.makeRDD(List(("a",4),("a",5),("a",6)))

    val joinRDD = rdd1.join(rdd2)
    joinRDD.collect().foreach(println)
    sc.stop()
  }
}
