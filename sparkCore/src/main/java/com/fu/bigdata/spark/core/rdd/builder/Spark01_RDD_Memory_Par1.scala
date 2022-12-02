package com.fu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)

    // TODO: 创建RDD
    //RDD的并行度 &分区
    val rdd = sc.makeRDD(List(1, 2, 3, 4,5),2)

    //将处理的数据保存成分区文件

    rdd.saveAsTextFile("output3")


    // TODO: 关闭环境
    sc.stop()
  }
}
