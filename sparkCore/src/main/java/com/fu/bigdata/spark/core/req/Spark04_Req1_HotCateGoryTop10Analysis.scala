package com.fu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Req1_HotCateGoryTop10Analysis {

  def main(args: Array[String]): Unit = {

    // TODO:  Top10 热门品类
    //02方法存在问题
    // TODO: 存在大量的reduceByKey 

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val actionRDD = sc.textFile("datas/user*")


    val acc = new HotCategoryAccumulator
    sc.register(acc,"hotCategory")
    //2.将数据转换结构
    //点击的场合，（品类id，（1，0，0））
    //下单的场合，（品类id，（0，1，0））
    //支付的场合，（品类id，（0，0，1））
    val flatRDD = actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          acc.add(datas(6),"click")
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add((id,"order"))
            }
          )
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.foreach(
            id => {
              acc.add((id,"pay"))
            }
          )
        }

      }
    )

    val accValue: mutable.Map[String, HotCategory] = acc.value
    val categories = accValue.map(_._2)

    val resultRDD: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        }
        else {
          false
        }
      }
    ).take(10)


    //3，将相同的品类ID的数据进行分组聚合
    //品类id，（点击数量，下单数量，支付数量））


    //4，将统计结果根据数量进行降序排列，取前10

    //5.打印结果
    resultRDD.foreach(println)

    sc.stop()
  }

  case class HotCategory(cid: String,var clickCount: Int,var orderCount: Int,var payCount: Int) {

  }

  //自定义累加器
  //IN:  (品类ID，行为类型）
  //OUT: mutable.Map[String,HotCategory]
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCount += 1
      }else if (actionType == "order"){
        category.orderCount +=1
      }else if (actionType == "pay") {
        category.payCount += 1
      }
      hcMap.update(cid,category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value

      map2.foreach{
        case (cid,hc)=>{
          val category = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCount +=hc.clickCount
          category.orderCount +=hc.orderCount
          category.payCount += hc.payCount
          map1.update(cid,category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }
}
