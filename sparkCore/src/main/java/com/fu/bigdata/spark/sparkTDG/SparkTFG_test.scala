package com.fu.bigdata.spark.sparkTDG

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkTFG_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)

    // val rdd = sc.makeRDD(List("spark", "scasdfsadfsala", "storm", "hadoop"))
    //
    //    def startWithS(s:String)={
    //      s.startsWith("s")
    //
    //    }
    //
    //    rdd.filter(line=>startWithS(line)).collect().foreach(println)


    //    def ifContains(word:String)={
    //    word.contains("北")
    //    }
    //    val rdd = sc.textFile("datas/city_info.txt")
    //    val mapRDD = rdd.flatMap(_.split("/t"))
    //    println(mapRDD.first())
    //    println("----------------R")
    //mapRDD.filter(word=>ifContains(word)).collect().foreach(println)


    //    def wordLengthReducer(left:String,right:String):String={
    //      if (left.length>right.length)
    //        left
    //      else right
    //    }
    //
    //    rdd.reduce(wordLengthReducer).foreach(print)


    //    val string="Spark The Definitive Guide work with hadoop".split(" ")
    //    val rdd = sc.makeRDD(string)
    //
    //    val chars = rdd.flatMap(word => word.toLowerCase.toSeq)
    //    val mapRDD = chars.map(word=>(word,1))
    //
    //    val resultRDD = mapRDD.groupByKey().mapValues(iter => {
    //      iter.toList.size
    //    })
    //    resultRDD.collect().foreach(println)


    val string = "Spark The Definitive Guide work with hadoop".split(" ")
    val rdd = sc.makeRDD(string, 2)

    val mapRDD = rdd.flatMap(_.toLowerCase().toSeq).map((_, 1))


    def maxFunc(left: Int, right: Int) = {
      math.max(left, right)
    }

    def addFunc(left: Int, right: Int) = {
      left + right
    }
mapRDD.foldByKey(0)(addFunc).collect().foreach(println)
    println("----------------")
    mapRDD.aggregateByKey(0)(addFunc,maxFunc).collect().foreach(println)

  }
}
