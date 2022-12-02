package com.fu.bigdata.spark.sparkTDG

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkTFG01_01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flight_data")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
//    val fligthData2015 = sparkSession.read
//                                      .option("inferSchema", "true")
//                                      .option("header", "true")
//                                      .csv("hdfs://localhost:8020/datas/flight_data_csv/2015-summary.csv")
//
//    fligthData2015.createOrReplaceTempView("flight_data_2015")
//
//    val sqlWay = sparkSession.sql(
//      """
//        |select DEST_COUNTRY_NAME,sum(count)
//        |as totalCount from flight_data_2015
//        |group by DEST_COUNTRY_NAME
//        |order by totalCount desc limit 5
//        |""".stripMargin)
//
//    fligthData2015.printSchema()

    val df = sparkSession.read.json("hdfs://localhost:8020/datas/flight_data_json/2015-summary.json")

    println(df.schema)



    sparkSession.stop()
  }

}
