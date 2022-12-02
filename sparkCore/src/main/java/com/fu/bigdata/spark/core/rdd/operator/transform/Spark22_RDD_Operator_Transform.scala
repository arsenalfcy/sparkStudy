package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 求平均值
    val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val rdd2 = sc.makeRDD(List(("a",4),("b",5)))

    val leftjoinRDD = rdd1.leftOuterJoin(rdd2)
    val rightjoinRDD = rdd1.rightOuterJoin(rdd2)
    leftjoinRDD.collect().foreach(println)
    rightjoinRDD.collect().foreach(println)
    sc.stop()
  }
}
