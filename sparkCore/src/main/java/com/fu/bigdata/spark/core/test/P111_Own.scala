package com.fu.bigdata.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object P111_Own {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Own_Exer")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("datas/user_visit_action1.txt")

    val flatRDD = rdd.flatMap(
      line => {
        val datas = line.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val orders = datas(8).split(",")
          orders.map(t => (t, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val pays = datas(10).split(",")
          pays.map(t => (t, (0, 0, 1)))
        } else Nil
      }
    )

    val reduceRDD = flatRDD.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }

    val resultRDD = reduceRDD.sortBy(_._2, false).take(10).map(_._1)

    val filterRDD = rdd.filter(
      line => {
        val datas = line.split("_")
        if(datas(6) != "-1")
        resultRDD.contains(datas(6))
        else false
      }
    )

    val mapRDD= filterRDD.map(
      line => {
        val datas = line.split("_")
        ((datas(6),datas(2)),1)
      }
    ).reduceByKey(_+_)

    val mapRDD2 = mapRDD.map {
      case ((cid, sid), sum) => (cid, (sid, sum))
    }
    val groupRDD = mapRDD2.groupByKey()
    groupRDD.mapValues(
      iter=>{
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    ).collect().foreach(println)
  }



}
