package com.fu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    //val reduceRDD = rdd.reduce(_ + _)
    //println(reduceRDD)
    var  sum = 0
    rdd.foreach(
      num=>{
        sum += num
        println(sum)    //打印结果：1,3,6,10
        sum
      }
    )

    println(sum)      //打印结果 0   想想为什么？
  }
}
