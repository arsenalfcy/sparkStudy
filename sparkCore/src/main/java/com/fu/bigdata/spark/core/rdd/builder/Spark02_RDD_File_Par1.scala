package com.fu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO: 创建RDD
    //数据分区分配
    //1，数据以行为单位读取。spark读取文件，采用hadoop的方式，一行一行读取，跟字节数没有关系
    val rdd = sc.textFile("datas/1.txt",2)
    rdd.saveAsTextFile("output4")
    // TODO: 关闭环境
    sc.stop()
  }
}
