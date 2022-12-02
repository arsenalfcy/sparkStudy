package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object P109_BC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("bc")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 4)))

    var map =mutable.Map(("a",3),("b",5),("c",6))
    val bc = sc.broadcast(map)

    rdd.map{
      case (w,c)=>{
        val i = bc.value.getOrElse(w,0)+c
        (w,i)
      }
    }.collect().foreach(println)


    sc.stop()
  }
}
