package com.fu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    // TODO: 建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    wordCount1(sc)
    wordCount2(sc)
    wordCount3(sc)
    wordCount4(sc)
    wordCount5(sc)
    wordCount9(sc)

    sc.stop()

  }

  //groupBy
  def wordCount1(sc:SparkContext)={
    val rdd = sc.makeRDD(List("Hello scala", "Hello spark"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val groupRDD = flatRDD.groupBy(word => word)
    val result: RDD[(String, Int)] = groupRDD.mapValues(
      iter => iter.toList.size
    )
    result.collect().foreach(println)
    println("---------------------------")
  }

  //groupByKey
  def wordCount2(sc:SparkContext)={
    val rdd = sc.makeRDD(List("Hello scala", "Hello spark","Hello hive"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val groupRDD = flatRDD.map((_,1)).groupByKey()
    val result: RDD[(String, Int)] = groupRDD.mapValues(
      iter => iter.toList.size
    )
    result.collect().foreach(println)
    println("---------------------------")
  }


  //reduceByKey
  def wordCount3(sc:SparkContext)={
    val rdd = sc.makeRDD(List("Hello scala", "Hello spark","Hello hive","Hello Kafka"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val groupRDD = flatRDD.map((_,1)).reduceByKey(_+_)

    groupRDD.collect().foreach(println)
    println("------------------")
  }

  //aggregateByKey
  def wordCount4(sc:SparkContext)={
    val rdd = sc.makeRDD(List("Hello scala", "Hello spark","Hello hive","Hello Kafka"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))
    val groupRDD = mapRDD.aggregateByKey(0)(_+_,_+_)
    groupRDD.collect().foreach(println)
    println("------------------")
  }

  //foldByKey
  def wordCount5(sc:SparkContext)={
    val rdd = sc.makeRDD(List("Hello scala", "Hello spark","Hello hive","Hello Kafka"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))
    val groupRDD = mapRDD.foldByKey(0)(_+_)
    groupRDD.collect().foreach(println)
    println("------------------")
  }

  //combineByKey
  def wordCount6(sc:SparkContext)={
    val rdd = sc.makeRDD(List("Hello scala", "Hello spark","Hello hive","Hello Kafka"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))
    val combineRDD = mapRDD.combineByKey(v => v, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y)
    combineRDD.collect().foreach(println)
    println("------------------")
  }

  //countByKey
  def wordCount7(sc:SparkContext)={
    val rdd = sc.makeRDD(List("Hello scala", "Hello spark","Hello hive","Hello Kafka"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))
    val countRDD = mapRDD.countByKey()

    println("------------------")
  }

  //countByValue
  def wordCount8(sc:SparkContext)={
    val rdd = sc.makeRDD(List("Hello scala", "Hello spark","Hello hive","Hello Kafka"))
    val flatRDD = rdd.flatMap(_.split(" "))

    val countRDD = flatRDD.countByValue()
    val countRDD2 = countRDD.mapValues(v => v.toInt)

    println("------------------")
  }

  //reduce
  def wordCount9(sc:SparkContext)={
    val rdd = sc.makeRDD(List("Hello scala", "Hello spark","Hello hive","Hello Kafka"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    val wordCount = mapRDD.reduce(
      (map1, map2) => {
        map2.foreach{
          case (word,count) =>{
            val newCount = map1.getOrElse(word,0L)+count
            map1.update(word,newCount)
          }
        }
        map1
      }
    )
    println(wordCount)
  }
}
