package com.fu.bigdata.spark.core.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// TODO: 针对方法1的优化
object P120_Exer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Exer")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("datas/user_visit_action.txt")
    // TODO: 给rdd增加缓存，避免重复读
    rdd.cache()
    val mapRDD = rdd.map(
      line => {
        val datas = line.split("_")
        new UserVisitAction(
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
    mapRDD.cache()

    //计算分母
    val pageIdToCount = mapRDD.map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap


    //计算分子
    //根据session分组
    val sessionRDD = mapRDD.groupBy(_.session_id)

    //分组后根据访问时间排序
    val mvRDD = sessionRDD.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        val flowIds = sortList.map(_.page_id)
        val rest=flowIds.takeRight(flowIds.size-1)
        val tuples = flowIds.zip(rest)
        tuples.map(t=>(t,1))
      }
    )
    val flatRDD = mvRDD.map(_._2).flatMap(list => list)
    val dataRDD = flatRDD.reduceByKey(_ + _)

    // TODO: 计算

    dataRDD.foreach{
      case((pageid1,pageid2),sum)=>{
        val pageSum = pageIdToCount.getOrElse(pageid1, 0L)
        println(s"页面${pageid1}跳转到${pageid2}的比率为："+(sum.toDouble/pageSum))
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
