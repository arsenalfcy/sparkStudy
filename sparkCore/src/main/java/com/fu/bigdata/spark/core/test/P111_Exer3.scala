package com.fu.bigdata.spark.core.test

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// TODO: 针对方法2的优化，自定义累加器，没有shuffle
object P111_Exer3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Exer")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("datas/user_visit_action.txt")
    // TODO: 给rdd增加缓存，避免重复读
    rdd.cache()

    // TODO: 使用flatmap方法一次性处理完成，只有一次shuffle

    val acc= new Top10Accumulator
      sc.register(acc,"hotCategory")
 rdd.foreach(
      line => {
        val datas = line.split("_")
        if (datas(6) != "-1") {
          acc.add(datas(6), "click")
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => acc.add(id, "order"))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => acc.add(id, "pay"))
        } else Nil
      }
    )
    val result= acc.value.map(_._2)
result.foreach(println)

    sc.stop()
  }

  case class Top10(id: String, var click: Int, var order: Int, var pay: Int)

  class Top10Accumulator extends AccumulatorV2[(String, String), mutable.Map[String, Top10]] {

    private val top10 = mutable.Map[String, Top10]()

    override def isZero: Boolean = {
      top10.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, Top10]] = {
      new Top10Accumulator
    }

    override def reset(): Unit = {
      top10.clear()
    }

    override def add(v: (String, String)): Unit = {
      val category: Top10 = top10.getOrElse(v._1, Top10(v._1, 0, 0, 0))
      v._2 match {
        case "click" => category.click += 1
        case "order" => category.order += 1
        case "pay" => category.pay += 1
      }

      top10.update(v._1,category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, Top10]]): Unit = {
      val map1= this.top10
      val map2=other.value
      map2.foreach{
        case(cid,hc)=>{
          val category = map1.getOrElse(cid, Top10(cid, 0, 0, 0))
          category.click+=hc.click
          category.order+=hc.order
          category.pay+=hc.pay

          map1.update(cid,category)
      }
      }
    }

    override def value: mutable.Map[String, Top10] = top10
  }
}
