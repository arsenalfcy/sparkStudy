package com.fu.bigdata.spark.core.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Aggregator

object P181_Exer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SQL_Exer")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()


    System.setProperty("HADOOP_USER_NAME", "fuchengyao")
    // TODO: 准备数据
    spark.sql("use fu")

//    spark.sql(
//      """
//        |CREATE TABLE `user_visit_action`(
//        | `date` string,
//        | `user_id` bigint,
//        | `session_id` string,
//        | `page_id` bigint,
//        | `action_time` string,
//        | `search_keyword` string,
//        | `click_category_id` bigint,
//        | `click_product_id` bigint,
//        | `order_category_ids` string,
//        | `order_product_ids` string,
//        | `pay_category_ids` string,
//        | `pay_product_ids` string,
//        | `city_id` bigint)
//        |row format delimited fields terminated by '\t';
//        |""".stripMargin)
//
//
//    spark.sql(
//      """
//        |load data local inpath 'datas/user_visit_action.txt' into table
//        |fu.user_visit_action;
//        |""".stripMargin)

//    spark.sql(
//      """
//        |CREATE TABLE `product_info`(
//        | `product_id` bigint,
//        | `product_name` string,
//        | `extend_info` string)
//        |row format delimited fields terminated by '\t';
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        |load data local inpath 'datas/product_info.txt' into table fu.product_info;
//        |""".stripMargin)
//
//
//    spark.sql(
//      """
//        |CREATE TABLE `city_info`(
//        | `city_id` bigint,
//        | `city_name` string,
//        | `area` string)
//        |row format delimited fields terminated by '\t';
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        |load data local inpath 'datas/city_info.txt' into table fu.city_info;
//        |""".stripMargin)
//
//spark.sql("show tables").show()
    //查询基本数据
    spark.sql(
      """
        |select c.*,d.area,d.city_name from
        |(select
        |  a.*,p.product_name
        |from user_visit_action a
        |join product_info p on a.click_product_id=p.product_id)c join city_info d on c.city_id=d.city_id
        |where click_product_id >-1
        |""".stripMargin).createOrReplaceTempView("t1")

    //根据区域商品进行聚合操作

    spark.sql(
      """
        |select
        |  area,
        |  product_name,
        |  count(*) as clickCount,
        |  cityRemark(city_name) as city_remark
        |from t1
        |group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |*,
        |rank() over(partition by area order by clickCount desc) as rank
        |from
        |t2
        |""".stripMargin).createOrReplaceTempView("t3")

  spark.sql(
    """
      |select
      |*
      |from
      |t3 where rank<=3
      |""".stripMargin).show()
  }


}
