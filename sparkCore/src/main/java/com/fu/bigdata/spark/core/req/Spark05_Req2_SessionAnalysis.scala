package com.fu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_SessionAnalysis {

  def main(args: Array[String]): Unit = {

    // TODO:  Top10 热门品类
    //02方法存在问题
    // TODO: 存在大量的reduceByKey

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val actionRDD = sc.textFile("datas/user*")
    actionRDD.cache()

    val top10Ids = top10Category(actionRDD)

    //1.过滤原始数据，保留点击前10品类ID
    val filterRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas = action.split("_")
        if(datas(6) != "-1"){
          top10Ids.contains(datas(6))
        }else{
          false
        }
      }
    )

    //2.根据品类ID和sessionID进行点击量的统计
    val reduceRDD = filterRDD.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    //3.将统计结果进行结构转换
    //（品类ID,sessionID),sum)=>(品类ID，（sessionID，sum））
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((cid, sid), sum) => (cid, (sid, sum))
    }
    //4.相同的品类进行分组
    val groupRDD = mapRDD.groupByKey()

    //5.将分组后的数据进行点击量排序，取前十
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)   //这里的take是scala的方法，不是spark的算子
      }
    )
    resultRDD.collect().foreach(println)
  }

  def top10Category(actionRDD:RDD[String]) ={
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
 reduceRDD.sortBy(_._2,false).take(10).map(_._1)
  }
}
