package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object Top10CategoryTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("top10")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/user*")

    val flatRDD = rdd.flatMap(
      line => {
        val datas = line.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id=>(id,(0,1,0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id=>(id,(0,0,1)))
        } else Nil
      }
    )

    val reduceRDD = flatRDD.reduceByKey(
      (data1, data2) => {
        (data1._1 + data2._1, data1._2 + data2._2, data1._3 + data2._3)
      }
    )
    val resultRDD = reduceRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)
  }
}
