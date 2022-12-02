package com.fu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCateGoryTop10Analysis {

  def main(args: Array[String]): Unit = {

    // TODO:  Top10 热门品类
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val actionRDD = sc.textFile("datas/user_visit_action1.txt")

    //2.统计品类的点击数量
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"        //如果点击的品类 ID 和产品 ID 为-1，表示数据不是点击数据,将其过滤掉，过滤后的数据给下一步使用
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    //3.统计品类的下单数量
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"            //如果本次不是下单行为，则数据采用 null 表示
      }
    )

    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val strings = datas(8).split(",")
        strings.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)


    //4.统计品类的支付数量

    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val strings = datas(10).split(",")
        strings.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    payCountRDD.collect().foreach(println)

    //5.将品类进行排序，并取前10名
    //点击数量排序，下单数量排序，支付数量排序
    //元组排序：先比较第一个，再比较第二个，再比较第三个，以此类推
    //（品类ID，（点击数量，下单数量，支付数量））
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCount = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCount = iter1.next()
        }
        var orderCount = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCount = iter2.next()
        }
        var payCount = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCount = iter3.next()
        }

        (clickCount, orderCount, payCount)
      }
    }

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    //6.打印结果
    resultRDD.foreach(println)

    sc.stop()
  }
}
