package com.fu.bigdata.spark.sparkTDG

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object SparkTFG14_01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("localhost[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val flights = spark.read.parquet("/Users/fuchengyao/Desktop/BigData/1.学习资料/spark/Spark-The-Definitive-Guide-master/data/flight-data/parquet/2010-summary.parquet").as[Flight]

    new LongAccumulator

  }
  case class Flight(DEST_COUNTRY_NAME:String,ORIGIN_COUNTRY_NAME:String,count:BigInt)
}
