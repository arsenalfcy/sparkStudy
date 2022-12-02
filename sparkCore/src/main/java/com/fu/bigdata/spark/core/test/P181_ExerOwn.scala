package com.fu.bigdata.spark.core.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object P181_ExerOwn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SQL_Exer")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()


    System.setProperty("HADOOP_USER_NAME", "fuchengyao")
    // TODO: 准备数据
    spark.sql("use fu")




  }
}
