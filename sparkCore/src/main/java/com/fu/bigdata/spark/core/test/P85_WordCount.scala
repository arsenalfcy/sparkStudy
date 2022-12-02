package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object P85_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hello spark", "hello scala"))
    val words = rdd.flatMap(_.split(" "))

    // TODO: groupBy
//    val groupRDD = words.groupBy(word=>word)
//    groupRDD.map(
//      iter=>{
//        (iter._1,iter._2.size)
//      }
//    ).collect().foreach(println)

    // TODO: groupByKey
//    val groupRDD = words.map((_, 1)).groupByKey()
//    val result = groupRDD.mapValues(v => v.size)
//    result.collect().foreach(println)

    // TODO: reduceByKey
//    words.map((_,1)).reduceByKey(_+_).collect().foreach(println)

    // TODO: aggregateByKey
//    words.map((_,1)).aggregateByKey(0)(_+_,_+_).collect().foreach(println)

    // TODO: flodByKey
//    words.map((_,1)).foldByKey(0)(_+_).collect().foreach(println)

    // TODO: combineByKey
//    words.map((_,1)).combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y).collect().foreach(println)

    // TODO: countByKey
//    val stringToLong: collection.Map[String, Long] = words.map((_, 1)).countByKey()
//    println(stringToLong)

    // TODO: countByValue
    val result = words.countByValue()
    println(result)


    sc.stop()
  }
}
