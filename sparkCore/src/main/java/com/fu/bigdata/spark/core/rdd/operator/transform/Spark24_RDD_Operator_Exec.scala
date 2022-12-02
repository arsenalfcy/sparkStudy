package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Operator_Exec {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/agent.log")
    val mapRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        ((datas(1),datas(4)),1)
      }
    )
    val reduceRDD = mapRDD.reduceByKey(_+_)
    val map2RDD = reduceRDD.map(data => (data._1._1, (data._1._2, data._2)))
    val groupRDD = map2RDD.groupByKey()
    val sortRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    sortRDD.collect().foreach(println)

    sc.stop()
  }
}
