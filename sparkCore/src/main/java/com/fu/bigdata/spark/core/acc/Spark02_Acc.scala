package com.fu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //获取系统累加器，spark默认提供了简单数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")
    rdd.foreach(
      num => {
        //使用累加器
        sumAcc.add(num)
      }
    )
    //获取累加器的值
    println(sumAcc.value)
    sc.stop()
  }
}
