package com.fu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

//早期UDAF强类型聚合函数使用DSL语法操作方法
object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {

    // TODO: 创建sparksql运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取mysql数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "Fcy8956741")
      .option("dbtable", "user")
      .load()


    df.show()

    //保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "Fcy8956741")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()

    spark.close()

  }


}
