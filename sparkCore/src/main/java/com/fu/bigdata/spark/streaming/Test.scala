package com.fu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ss = SparkSession.builder().config(conf).getOrCreate()

    val df = ss.range(Int.MaxValue.toLong + 100).selectExpr(s"1 as a", "id as b")
    val windowSpec = Window.partitionBy("a").orderBy("b")
    ss.time(df.select(col("a"), col("b"), row_number().over(windowSpec).alias("rn")).orderBy(desc("a"), desc("b")).select((col("rn") < 0).alias("dir")).groupBy("dir").count.show(20))
  }
}
