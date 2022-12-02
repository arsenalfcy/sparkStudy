package com.fu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object Spark03_SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {

    // TODO: 创建sparksql运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    df.show()

    spark.udf.register("MyAvg",new MyAvgUDAF())

    spark.sql("select MyAvg(age) from user").show()




    // TODO: 执行逻辑操作

    // TODO: 关闭环境
    spark.close()
  }

  //自定义聚合函数类：计算年龄平均值
  //继承UserDefinedAggregateFunction
  class MyAvgUDAF extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = {
      //输入数据结构
      StructType(
        Array(
          StructField("age",LongType)
        )
      )
    }

    override def bufferSchema: StructType = {
    //缓冲区数据结构
      StructType(
        Array(
          StructField("total",LongType),
          StructField("count",LongType)
        )
      )
    }

    //函数计算结果类型:Out
    override def dataType: DataType = LongType

    //函数稳定性
    override def deterministic: Boolean = true

    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0)=0L
      buffer(1)=0L
    }

    //根据输入的值来更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,buffer.getLong(0)+input.getLong(0))
      buffer.update(1,buffer.getLong(1)+1)
    }

    //缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
    }

    //计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0)/buffer.getLong(1)
    }
  }

}
