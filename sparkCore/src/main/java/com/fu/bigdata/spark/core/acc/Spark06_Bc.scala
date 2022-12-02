package com.fu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// TODO: 使用广播变量，一个executor只需要保存一份数据，多个task共享 
object Spark06_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))


    val map = mutable.Map(("a",4),("b",5),("c",6))
    val bc = sc.broadcast(map)
    rdd1.map{
      case (w,c) => {
        val l = bc.value.getOrElse(w, 0)
        (w,(c,l))
      }
    }.collect().foreach(println)


    sc.stop()
  }
}
