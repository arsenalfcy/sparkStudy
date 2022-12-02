package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd = sc.textFile("datas/apache.log")
    val mapRDD: RDD[(String,Int)] = rdd.map(
      data => {
        val datas = data.split(" ")
        val str = datas(3).substring(11, 13)
        (str,1)
      }
    )
//    val result = mapRDD.reduceByKey(_ + _)
//    result.collect().foreach(println)
val groupRDD = mapRDD.groupBy(_._1)
    groupRDD.map{
      case (hour,iter)=>{
        (hour,iter.size)
      }
    }.collect().foreach(println)


    sc.stop()
  }
}
