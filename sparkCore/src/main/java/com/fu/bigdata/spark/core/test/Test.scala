package com.fu.bigdata.spark.core.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val ds: Dataset[String] = spark.read.text("datas/city_info.txt").as[String]

    ds.flatMap(_.split("\t")).groupBy("value").count().show()
  }
}
