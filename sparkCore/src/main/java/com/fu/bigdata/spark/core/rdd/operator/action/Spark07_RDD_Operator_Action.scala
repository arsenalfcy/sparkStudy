package com.fu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    val user = new User()
    rdd.foreach(
      num => {
        println("age="+(user.age+num))
      }
    )


    sc.stop()
  }
//  class User() extends Serializable {
  case class User(){      //样例类会自动实现序列化
    var age = 30
  }

}
