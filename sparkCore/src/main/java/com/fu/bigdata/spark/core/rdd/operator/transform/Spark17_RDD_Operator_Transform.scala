package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子groupby
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)
    val aggreRDD = rdd.aggregateByKey(0)((x, y) => {
      math.max(x, y)
    }, (x, y) => x + y)
    aggreRDD.collect().foreach(println)
    sc.stop()
  }
}
