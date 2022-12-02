package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

// TODO: 针对方法1的优化
object P111_Exer2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Exer")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("datas/user_visit_action.txt")
    // TODO: 给rdd增加缓存，避免重复读
    rdd.cache()

    // TODO: 使用flatmap方法一次性处理完成，只有一次shuffle
    val flatRDD = rdd.flatMap(
      line => {
        val datas = line.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else Nil
      }
    )
    flatRDD.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    }).sortBy(_._2, false).take(10).foreach(println)

    sc.stop()
  }
}
