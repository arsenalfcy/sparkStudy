package com.fu.bigdata.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object P120_Exerown {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Exer")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("datas/user_visit_action1.txt")

    val mapRDD = rdd.map(
      line => {
        val datas = line.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )

    //求点击sum
    val pageToCount = mapRDD.map(
      t => {
        (t.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    //求分子
    val mvRDD = mapRDD.groupBy(_.session_id).mapValues {
      t => {
        val actions = t.toList.sortBy(_.action_time)
        val pageId = actions.map(_.page_id)
        val tail = pageId.tail
        val tuples = pageId.zip(tail)
       tuples.map(t=>(t,1L))
      }
    }.map(_._2).flatMap(list=>list).reduceByKey(_+_).collect()

    mvRDD.foreach{
      case((p1,p2),s1)=>{
        val sum = pageToCount.getOrElse(p1, 0L)
        println(s"页面${p1}跳转到${p2}的概率为："+(s1.toDouble/sum))
      }
    }

    sc.stop()
  }
  case class UserVisitAction(
                              date: String,//用户点击行为的日期
                              user_id: Long,//用户的 ID
                              session_id: String,//Session 的 ID
                              page_id: Long,//某个页面的 ID
                              action_time: String,//动作的时间点
                              search_keyword: String,//用户搜索的关键词
                              click_category_id: Long,//某一个商品品类的 ID
                              click_product_id: Long,//某一个商品的 ID
                              order_category_ids: String,//一次订单中所有品类的 ID 集合
                              order_product_ids: String,//一次订单中所有商品的 ID 集合
                              pay_category_ids: String,//一次支付中所有品类的 ID 集合
                              pay_product_ids: String,//一次支付中所有商品的 ID 集合
                              city_id: Long
                            )//城市 id
}
