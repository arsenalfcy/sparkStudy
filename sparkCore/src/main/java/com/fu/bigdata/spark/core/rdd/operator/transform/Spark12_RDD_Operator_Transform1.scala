package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd = sc.makeRDD(List(("1",1),("11",2),("2",3)),2)

    val sortRDD = rdd.sortBy(tuple=>tuple._1.toInt,false)
    sortRDD.collect().foreach(println)





    sc.stop()
  }
}
