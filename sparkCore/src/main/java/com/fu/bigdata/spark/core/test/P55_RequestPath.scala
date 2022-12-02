package com.fu.bigdata.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object P55_RequestPath{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RP")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("datas/apache.log")
    val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")

    val mapRDD = rdd.map(
      line => {

        val datas = line.split(" ")
        val date = sdf.parse(datas(3))
        val date1 = sdf1.format(date)
        (date1,datas(6))
      }
    )
    val filterRDD = mapRDD.filter(data => data._1 == "2015-05-17")
    filterRDD.collect().foreach(println)


    sc.stop()
  }
}
