package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object P111_Exer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Exer")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("datas/user_visit_action.txt")

    val filterRDD = rdd.filter(
      line => {
        val datas = line.split("_")
        datas(6) != "-1"
      }
    )

    val mapRDD = filterRDD.map{
      line=>{
        val datas = line.split("_")
        (datas(6),1)
      }
    }
    val clickCount = mapRDD.reduceByKey(_ + _)

    val filterRDD2 = rdd.filter(
      line => {
        val datas = line.split("_")
        datas(8) != "null"
      }
    )

    val mapRDD2 = filterRDD2.flatMap{
      line=>{
        val datas = line.split("_")
        val orderId = datas(8).split(",")
        orderId.map(id=>(id,1))
      }
    }
    val orderCount = mapRDD2.reduceByKey(_ + _)


    val filterRDD3 = rdd.filter(
      line => {
        val datas = line.split("_")
        datas(10) != "null"
      }
    )

    val mapRDD3= filterRDD3.flatMap{
      line=>{
        val datas = line.split("_")
        val orderId = datas(10).split(",")
        orderId.map(id=>(id,1))
    }}
    val payCount = mapRDD3.reduceByKey(_ + _)

    val cogroupRDD = clickCount.cogroup(orderCount, payCount)

    val resultRDD = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        var orderCnt = 0
        var payCnt = 0

        if (clickIter.iterator.hasNext) clickCnt = clickIter.iterator.next()
        if (orderIter.iterator.hasNext) orderCnt = orderIter.iterator.next()
        if (payIter.iterator.hasNext) payCnt = payIter.iterator.next()

        (clickCnt, orderCnt, payCnt)
      }
    }

    val result = clickCount.join(orderCount)
    result.collect().foreach(println)
    resultRDD.sortBy(_._2,false).collect().foreach(println)





    sc.stop()
  }
}
