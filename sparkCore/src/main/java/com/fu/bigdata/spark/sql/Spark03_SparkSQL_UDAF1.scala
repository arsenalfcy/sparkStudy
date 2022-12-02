package com.fu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}


object Spark03_SparkSQL_UDAF1 {
  def main(args: Array[String]): Unit = {

    // TODO: 创建sparksql运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    df.show()

    spark.udf.register("MyAvg", functions.udaf(new MyAvgUDAF()))

    spark.sql("select MyAvg(age) from user").show()




    // TODO: 执行逻辑操作

    // TODO: 关闭环境
    spark.close()
  }

  //自定义聚合函数类：计算年龄平均值
  //继承UserDefinedAggregateFunction
  //package org.apache.spark.sql.expressions.Aggregator   不要导错包
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    //缓冲区初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //根据输入数据更新缓冲区数据
    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total + a
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
    override def bufferEncoder: Encoder[Buff] =Encoders.product

    //输出编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
