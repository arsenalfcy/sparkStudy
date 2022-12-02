package com.fu.bigdata.spark.core.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DatasetTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    //启用hive支持enableHiveSupport()
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    import spark.implicits._

    val flightDF: DataFrame = spark.read.json("hdfs://localhost:8020/data/flight_data/json/")
    val flights = flightDF.as[Flight]
flightDF.printSchema()
    flights.show(4)
    println(flights.schema)
    println("----------")
    flightDF.createOrReplaceTempView("dfTable")

    println(flightDF.first())

    flightDF.select("DEST_COUNTRY_NAME").show(2)

    flightDF.selectExpr("*","(DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME) as withinCountry").show(2)

    spark.sql("select *,(DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME)as inCountry from dfTable limit 2").show()

    flightDF.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))").show(5)

  }

  case class Flight(DEST_COUNTRY_NAME:String,ORIGIN_COUNTRY_NAME:String,count:BigInt)
}
