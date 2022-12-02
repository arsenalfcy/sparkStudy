package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 双value类型操作
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val mapRDD = rdd.map((_,1))
    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")




    sc.stop()
  }
}
