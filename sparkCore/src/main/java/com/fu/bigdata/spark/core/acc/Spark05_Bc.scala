package com.fu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// TODO: 不使用广播变量，每个task都要传一份map数据 
object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))

//    val rdd2 = sc.makeRDD(List(
//      ("a",4),("b",5),("c",6)
//    ))

    //join会导致数据量几何增长，并且会影响shuffle性能，不推荐使用
//    val joinRdd = rdd1.join(rdd2)
//    joinRdd.collect().foreach(println)

    val map = mutable.Map(("a",4),("b",5),("c",6))
    rdd1.map{
      case (w,c)=>{
        val l = map.getOrElse(w, 0)
        (w,(c,l))
      }
    }.collect().foreach(println)


    sc.stop()
  }
}
