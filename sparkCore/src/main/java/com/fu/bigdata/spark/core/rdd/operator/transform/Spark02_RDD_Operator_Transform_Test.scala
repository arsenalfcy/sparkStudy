package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.math.Ordering.comparatorToOrdering

object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
mapRDD.collect().foreach(println)
    sc.stop()
  }
}
