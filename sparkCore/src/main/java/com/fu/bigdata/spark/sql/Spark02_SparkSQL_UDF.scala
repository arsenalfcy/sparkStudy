package com.fu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {

    // TODO: 创建sparksql运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //自定义一个函数，在每行姓名前加一个前缀
    spark.udf.register("prefixName",(name:String)=>{
      "Name:"+name
    })
    spark.sql("select age,prefixName(username) from user").show()


    // TODO: 执行逻辑操作

    // TODO: 关闭环境
    spark.close()
  }

}
