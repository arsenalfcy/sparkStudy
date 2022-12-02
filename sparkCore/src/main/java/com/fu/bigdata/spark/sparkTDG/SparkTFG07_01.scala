package com.fu.bigdata.spark.sparkTDG

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, dense_rank, first, last, max, mean, rank, row_number, sum, sumDistinct, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object SparkTFG07_01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val conf = sparkConf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    //启用hive支持enableHiveSupport()
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema",true)
      .load("hdfs://localhost:8020/datas/retail_data_all/*.csv")
      .coalesce(5)

    df.cache()
    df.createOrReplaceTempView("dfTable")


    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))

    val windowSpec = Window.partitionBy("CustomerId", "date").orderBy(col("Quantity").desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)
    val rowNumRank=row_number().over(windowSpec)

    dfWithDate.where("CustomerId is not null").orderBy("CustomerId").select(col("CustomerId"),
     col("date"),
      col("Quantity"),
      purchaseRank.alias("quantityRank"),
      purchaseDenseRank.alias("quantityDenseRank"),
      maxPurchaseQuantity.alias("maxPurchaseQuantity"),
      rowNumRank
      ).show()


    spark.sql("select Country,sum(Quantity) as sumforCountry from dfTable group by Country order by sumforCountry desc").show()


    //dataframe
//    println(df.count())
//    df.select(countDistinct("StockCode")).show()
//    df.select(approx_count_distinct("stockCode",0.1)).show()
//    df.select(first("stockcode"),last("stockcode")).show()
//    df.select(functions.min("quantity"),max("quantity")).show()
//    df.select(sum("quantity")).show()
//    df.select(sumDistinct("quantity")).show()
//    df.select(
//      sum("quantity").alias("sumq"),
//      count("quantity").alias("countq"),
//      avg("quantity").alias("avgq"),
//      mean("quantity").alias("meanq")
//    ).selectExpr("sumq/countq","avgq","meanq").show




    //sparksql
//    spark.sql("select count(distinct stockcode) from dftable").show()
//    spark.sql("select approx_count_distinct(stockcode,0.1) as approxDis from dftable").show
//    spark.sql("select first(stockcode),last(stockcode) from dftable").show()
//    spark.sql("select min(quantity),max(quantity) from dftable").show()
//    spark.sql("select sum(quantity) from dftable").show()


  }
}
