package com.fu.bigdata.spark.core.rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("nba","xxxxxxx"),
      ("cba","xxxxxxx"),
      ("wnba","xxxxxxx"),
      ("nba","xxxxxxx"),
    ))

    val partitioner = new MyPartitioner
    val partRDD = rdd.partitionBy(partitioner)
    partRDD.saveAsTextFile("output3")


    sc.stop()
  }
  //自定义分区器
  //重写方法
  class MyPartitioner extends Partitioner{
    //分区数量
    override def numPartitions: Int = 3

    //返回分区索引，从0开始
    override def getPartition(key: Any): Int = {

      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }
}
