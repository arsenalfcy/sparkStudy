package com.fu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageFlowAnalysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)
    val actionRDD = sc.textFile("datas/user_visit_action1.txt")

    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
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
    actionRDD.cache()

    // TODO: 对指定页面连续跳转进行统计
    val ids =List(1,2,3,4,5,6,7)
    val okFlowIds:List[(Long,Long)] = ids.zip(ids.tail).map(t=>(t._1.toLong,t._2.toLong))


    // TODO: 计算分母
    val pageToCountMap = actionDataRDD.filter(
      action=>{ids.init.contains(action.page_id)}
    ).map(
      action => (action.page_id, 1L)
    ).reduceByKey(_ + _).collect().toMap
    pageToCountMap.foreach(println)


    // TODO: 计算分子

    //根据session进行分组
    val sessionRDD = actionDataRDD.groupBy(_.session_id)

    //分组后，根据访问时间进行排序
    val mvRDD= sessionRDD.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)

        val flowIds = sortList.map(_.page_id)

        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

        //将不合法的页面跳转进行过滤
        pageflowIds.filter(
          t =>{okFlowIds.contains(t)}
        ).map(
          t =>(t,1)
        )
      }
    )

    val flatRDD = mvRDD.map(_._2).flatMap(list => list)

    val dataRDD = flatRDD.reduceByKey(_ + _)
    dataRDD.collect().foreach(println)

    // TODO: 计算单跳转换率
    //分子除以分母
    dataRDD.foreach{
      case ((pageid1,pageid2),sum) => {
        val long = pageToCountMap.getOrElse(pageid1, 0L)
        println(s"页面${pageid1}跳转到${pageid2}换率为"+(sum.toDouble/long))
      }
    }
    sc.stop()
  }

  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的 ID
                              session_id: String, //Session 的 ID
                              page_id: Long, //某个页面的 ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的 ID
                              click_product_id: Long, //某一个商品的 ID
                              order_category_ids: String, //一次订单中所有品类的 ID 集合
                              order_product_ids: String, //一次订单中所有商品的 ID 集合
                              pay_category_ids: String, //一次支付中所有品类的 ID 集合
                              pay_product_ids: String, //一次支付中所有商品的 ID 集合
                              city_id: Long //城市 id
                            )

}
