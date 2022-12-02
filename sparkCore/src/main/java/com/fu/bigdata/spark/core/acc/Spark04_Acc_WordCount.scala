package com.fu.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("Hello", "spark","Hello", "scala"))

    //累加器
    //创建累加器对象，向spark注册

    val wcAcc = new MyAccumulator
    sc.register(wcAcc,"wordCountAcc")



    rdd.foreach(
      word =>{
wcAcc.add(word)
      }
    )

    println(wcAcc.value)
    sc.stop()
  }
  //重写方法
  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]]{
    private var wcMap = mutable.Map[String,Long]()

    //判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    //获取累加器需要计算的值
    override def add(v: String): Unit = {
      val newCount = wcMap.getOrElse(v, 0L) + 1
      wcMap.update(v,newCount)
    }

    //driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach{
        case (word,count) =>{
          val newCount = map1.getOrElse(word,0L)+count
          map1.update(word,newCount)
        }
      }
    }

    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
