package com.fu.bigdata.spark.sparkTDG

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.sql.SparkSession

object SparkTGF13_01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("par")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Users/fuchengyao/Desktop/BigData/1.学习资料/spark/Spark-The-Definitive-Guide-master/data/retail-data/all")
    val rdd = df.coalesce(10).rdd

//    rdd.map(line=>line(6)).take(5).foreach(println)
val keyRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)
//    keyRDD.partitionBy(new HashPartitioner(10)).take(10).foreach(println)
    keyRDD.partitionBy(new DomainPartitoner).map(_._1).glom().map(_.toSet.toSeq.length).take(5).foreach(println)

  }

  class DomainPartitoner extends Partitioner{
    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      val customerId=key.asInstanceOf[Double].toInt
      if (customerId==17850.0 || customerId==12583.0){
        0
      }else  new java.util.Random().nextInt(2)+1
    }
  }
}
