package com.fu.bigdata.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, SparkConf, SparkContext}

object P103_Partitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("arsenal", "jesus"),
      ("mancity", "deburoney"),
      ("manunit", "ronaldo"),
      ("arsenal", "veira")))
    val parRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitoner)

    parRDD.saveAsTextFile("datas/arsenal")

    sc.stop()


  }
  class MyPartitoner extends Partitioner {
    override def numPartitions: Int = 2

    override def getPartition(key: Any): Int = {
    key match {
      case "arsenal" =>0
      case _ =>1
    }
    }
  }
}
