package com.fu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism","5")
    val sc = new SparkContext(sparkConf)

    // TODO: 创建RDD
    //RDD的并行度 &分区
//    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //将处理的数据保存成分区文件
//    rdd.saveAsTextFile("output")
//    rdd.saveAsTextFile("output1")
    rdd.saveAsTextFile("output2")


    // TODO: 关闭环境
    sc.stop()
  }
}
