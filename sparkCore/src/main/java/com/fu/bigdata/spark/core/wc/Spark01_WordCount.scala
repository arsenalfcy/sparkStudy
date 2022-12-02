package com.fu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    // TODO: 建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO: 执行业务操作
    //1.读取文件，读取一行一行的数据
    val lines = sc.textFile("datas")

    //2.将一行数据进行拆分，形成一个一个的单词
    val words = lines.flatMap(_.split(" "))

    //3.将数据根据单词进行分组，便于统计
    val groupedWords = words.groupBy(word => word)

    //4.对分组后的数据进行转换
    val result: RDD[(String, Int)] = groupedWords.map(word => (word._1, word._2.size))

    //5.将转换结果采集到控制台打印
    val tuples = result.collect()
    tuples.foreach(println)
    // TODO: 关闭连接
    sc.stop()

  }
}
