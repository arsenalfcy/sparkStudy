package com.fu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req1_HotCateGoryTop10Analysis {

  def main(args: Array[String]): Unit = {

    // TODO:  Top10 热门品类
    //02方法存在问题
    // TODO: 存在大量的reduceByKey 

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val actionRDD = sc.textFile("datas/user*")


    //2.将数据转换结构
    //点击的场合，（品类id，（1，0，0））
    //下单的场合，（品类id，（0，1，0））
    //支付的场合，（品类id，（0，0，1））
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        }
        else Nil
      }
    )

    val reduceRDD = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    val resultRDD: Array[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2,false).take(10)

    //3，将相同的品类ID的数据进行分组聚合
    //品类id，（点击数量，下单数量，支付数量））


    //4，将统计结果根据数量进行降序排列，取前10

    //5.打印结果
    resultRDD.foreach(println)

    sc.stop()
  }
}
