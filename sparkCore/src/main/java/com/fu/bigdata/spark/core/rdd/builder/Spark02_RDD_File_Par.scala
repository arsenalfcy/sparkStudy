package com.fu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO: 创建RDD
    //从文件中创建RDD，将文件中的数据作为处理的数据源
    val rdd = sc.textFile("datas/1.txt",3)
    rdd.saveAsTextFile("output4")
    // TODO: 关闭环境
    sc.stop()
  }
}
