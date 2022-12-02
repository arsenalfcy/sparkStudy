package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object P78_Exer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Exer")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("datas/agent.log")

    val mapRDD = rdd.map(
      line => {
val datas = line.split(" ")
        ((datas(1),datas(4)),1)
      }
    )
//    val groupRDD = mapRDD.groupByKey()



    val reduceRDD = mapRDD.reduceByKey(_+_)
    val mapRDD2 = reduceRDD.map(
      line => ((line._1._1), (line._1._2, line._2))
    )
    val groupRDD = mapRDD2.groupByKey()

    val sortRDD = groupRDD.mapValues(
      line => {
        line.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    sortRDD.collect().foreach(println)


  }
}
