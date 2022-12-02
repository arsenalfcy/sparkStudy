package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 双value类型操作
    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",4),("a",5),("b",3)))

    val reduceRDD = rdd.reduceByKey((num1, num2) => {
      println(s"num1=${num1}  num2=${num2}")
      num1 + num2
    })

    reduceRDD.collect().foreach(println)


    sc.stop()
  }
}
