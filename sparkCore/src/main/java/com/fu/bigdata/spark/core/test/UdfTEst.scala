package com.fu.bigdata.spark.core.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object UdfTEst {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("udfTest")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val tuples: Seq[(Int, String)] = Seq((1, "aaa"), (2, "bbb"), (3, "ccc"))
    val df: DataFrame = tuples.toDF("id", "name")

//    spark.udf.register("touppercase",df:DataFrame=>)
  }
}
