package com.fu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 求平均值
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4),("b",5),("a",6)),2)

    val newRDD:RDD[(String,(Int,Int))] = rdd.combineByKey(
      v=>(v,1),
      (t:(Int,Int),v)=>{
        (t._1+v,t._2+1)
      },
      (t1:(Int,Int),t2:(Int,Int))=>{
      (t1._1+t2._1,t1._2+t2._2)
    })
    val value: RDD[(String, Int)] = newRDD.mapValues(data => {
      (data._1 / data._2)
    })
    value.collect().foreach(println)
    sc.stop()
  }
}
