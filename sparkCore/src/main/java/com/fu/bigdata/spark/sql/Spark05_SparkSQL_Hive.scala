package com.fu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._


object Spark05_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {

    // TODO: 创建sparksql运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    //启用hive支持enableHiveSupport()
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    spark.sql("show tables").show()

   //使用spark-sql连接外置hive



    spark.close()

  }


}
