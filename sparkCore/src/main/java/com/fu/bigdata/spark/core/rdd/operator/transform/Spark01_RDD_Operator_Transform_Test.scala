package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd = sc.textFile("datas/apache.log")

    //长的字符串转换为短的
    val mappRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )
    mappRDD.collect().foreach(println)



    sc.stop()
  }
}
