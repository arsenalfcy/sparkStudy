package com.fu.bigdata.spark.sparkTDG

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkTFG11_01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flight")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val df = spark.read.parquet("hdfs://localhost:8020/datas/flight_data_parquet/*")
    val flights = df.as[Flight]

    flights.show(2)

    def isSame(flight: Flight):Boolean={
    flight.DEST_COUNTRY_NAME==flight.ORIGIN_COUNTRY_NAME
    }

    flights.filter(isSame(_)).show()

  }
case class Flight(DEST_COUNTRY_NAME:String,ORIGIN_COUNTRY_NAME:String,count:BigInt)
}
