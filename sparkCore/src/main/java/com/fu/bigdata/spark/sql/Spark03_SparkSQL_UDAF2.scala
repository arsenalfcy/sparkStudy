package com.fu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

//早期UDAF强类型聚合函数使用DSL语法操作方法
object Spark03_SparkSQL_UDAF2 {
  def main(args: Array[String]): Unit = {

    // TODO: 创建sparksql运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")


    import spark.implicits._
    val ds: Dataset[User] = df.as[User]

    //将udaf函数转换为查询的列对象
    val udafCol = new MyAvgUDAF().toColumn

    ds.select(udafCol).show

    // TODO: 执行逻辑操作

    // TODO: 关闭环境
    spark.close()
  }

  //自定义聚合函数类：计算年龄平均值
  //继承UserDefinedAggregateFunction
  //package org.apache.spark.sql.expressions.Aggregator   不要导错包
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
    //缓冲区初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //根据输入数据更新缓冲区数据
    override def reduce(b: Buff, a: User): Buff = {
      b.total = b.total + a.age
      b.count = b.count + 1
      b
    }

    //合并
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    //计算
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    //缓冲区编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  case class User(username: String, age: Long)

}
