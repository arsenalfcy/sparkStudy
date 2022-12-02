package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object AgentLogTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/agent.log")

    val reduceRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    ).reduceByKey(_ + _)

    val mapRDD = reduceRDD.map {
      case ((pro, id), count) => (pro, (id, count))
    }

    val groupRDD = mapRDD.groupByKey()
    val mapRDD2 = groupRDD.mapValues(
      data => data.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )
   mapRDD2.collect().foreach(println)

    sc.stop()

  }
}
