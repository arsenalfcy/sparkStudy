package com.fu.bigdata.spark.sparkTDG

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, date_format, date_sub, datediff, explode, months_between, split, struct, to_date}


object SparkTFG06_01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("chapter06")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("hdfs://localhost:8020/datas/retail_data_byday/2010-12-02.csv")


    val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))

//    df.select("*").show(false)
//
//    df.withColumn("splitted",split(col("Description")," "))
//      .withColumn("exploded",explode(col("splitted")))
//      .select("Description","InvoiceNo","exploded").show(false)


    val df1 = spark.range(5).toDF("num")


    val power = spark.udf.register("power3", (number:Double)=>{number*number*number})

    df1.select(power(col("num"))).show()

    def power3(num:Double):Double={
      num*num*num
    }

    spark.udf.register("power31",power3(_:Double))
    spark.sql("select power31(5)").show()

    spark.sql("show user functions").show()


//    val dateDF = spark.range(10)
//      .withColumn("today",current_date())
//      .withColumn("now",current_timestamp())
//
//    dateDF.createOrReplaceTempView("dateTable")
//
//    val dateAgo = dateDF.withColumn("fiveDaysAgo", date_sub(col("today"), 35))
//    dateAgo.select(datediff(col("today"),col("fiveDaysAgo"))).show()
//    dateAgo.select(months_between(col("fiveDaysAgo"),col("today"))).show()

    spark.stop()
  }
}
