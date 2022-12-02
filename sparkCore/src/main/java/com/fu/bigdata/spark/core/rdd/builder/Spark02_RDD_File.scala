package com.fu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO: 创建RDD
    //从文件中创建RDD，将文件中的数据作为处理的数据源
    //path的路径默认以当前环境的根路径为基准，可以写绝对路径，也可以写相对路径

    val rdd: RDD[String] = sc.textFile("hdfs://localhost:8020/data")
    rdd.collect().foreach(println)
    // TODO: 关闭环境
    sc.stop()
  }
}
